-- Sort member events in descending order (newest first), where rn = 1 represents the most recent event. Use this to identify member actioned cancellation events that occurred prior to termination. 
WITH
mbr_tick_events AS ( 
    SELECT
          fm.SK_TICKET
        , fm.BOOKINGITEMID
        , fm.SK_EVENTTYPE 
        , dm.EVENTTYPE
        , fm.SK_DATE
        , fm.SK_TIME
        , fm.VALUE 
        , fm.FLAG_REFUNDED
        , dm.ACTIVE
        , ROW_NUMBER() OVER (
            PARTITION BY fm.SK_TICKET
            ORDER BY fm.SK_DATE DESC, fm.SK_TIME DESC, dm.SORTORDER2 DESC
        ) AS rn
    FROM GOLD_DB.DW.FACTMEMBERSHIPPASSEVENTS fm
    JOIN GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING(SK_EVENTTYPE)
),
--For each member sk_ticket, retain only the most recent event (where rn = 1). 
mbr_tick_last_event AS (
    SELECT 
          SK_TICKET
        , ACTIVE AS MBR_TICK_STATUS
        , SK_EVENTTYPE AS LAST_SK_EVENT 
        , EVENTTYPE AS LAST_EVENT
        , SK_DATE AS LAST_SK_DATE
    FROM mbr_tick_events
    WHERE rn = 1 
),
--For each member sk_ticket, capture the event immediately preceding the most recent event (where rn = 2).
mbr_tick_prev_event AS (
    SELECT 
          SK_TICKET
        , ACTIVE AS MBR_TICK_STATUS
        , SK_EVENTTYPE AS PREV_SK_EVENT 
        , EVENTTYPE AS PREV_EVENT
        , SK_DATE AS PREV_SK_DATE
    FROM mbr_tick_events
    WHERE rn = 2 
),
--For each member sk_ticket, identify the create date as the join date.
mbr_tick_join_event AS (
    SELECT
          SK_TICKET
        , BOOKINGITEMID
        , SK_DATE AS JOIN_SK_DATE
    FROM mbr_tick_events
    WHERE EVENTTYPE = 'Creation' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SK_TICKET
        ORDER BY SK_DATE ASC, SK_TIME ASC
    ) = 1
),
--For each member sk_ticket, identify the inital payment dues. Use Qualify to guarantee there are not multiple initial payments. Creation and initial payment occur at the same time so it doesnt matter which event we pull. 
mbr_tick_initial_pay AS (
    SELECT
          SK_TICKET
        , ROUND(VALUE, 2) AS INITIAL_PAYMENT
    FROM mbr_tick_events
    WHERE EVENTTYPE = 'Initial Payment'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SK_TICKET
        ORDER BY SK_DATE ASC, SK_TIME ASC
    ) = 1
),
--For each member sk_ticket, identify average recurring payment dues.
mbr_tick_recurring_dues AS (
    SELECT 
          SK_TICKET 
        , COUNT(DISTINCT DATE(SK_DATE)) AS RECURR_PAY_COUNT
        , ROUND(AVG(VALUE), 2) AS RECURR_AVG_DUES
        , MAX(SK_DATE) AS LAST_RECURR_PAY_DATE
    FROM mbr_tick_events
    WHERE EVENTTYPE = 'Recurring Payment'
    GROUP BY SK_TICKET
),
--Track only the most recent upgrade event per ticket. Capture the upgrade on the parent sk_ticket only; the associated child sk_ticket should not reflect a separate upgrade event.
--Use rn = 1 to ensure the upgrade is the ticket's current state — tickets with a buried upgrade event followed by continued activity (e.g. recurring payments) are genuinely active and should not be flagged.
mbr_tick_upgrade_event AS (
    SELECT
          SK_TICKET
        , SK_DATE AS UPGRADE_DATE
    FROM mbr_tick_events
    WHERE rn = 1
        AND EVENTTYPE IN ('Upgraded From Active', 'Upgraded From Inactive')
),
--For each member sk_ticket, find the max refund request date and number of times a member has requested a refund 
mbr_tick_refund_event AS (
    SELECT
          SK_TICKET
        , MAX(SK_DATE) AS MAX_REFUND_SK_DATE
        , COUNT(DISTINCT SK_DATE) AS REFUND_REQUESTS
    FROM mbr_tick_events
    WHERE EVENTTYPE = 'Refund'
    GROUP BY SK_TICKET
),
-- CTE - Capture the cancel reason and the effective cancel date. Note that memberships may remain active through the end of the monthly term.
-- FIRST SELECT - Identify members for cancellation when their last event is 'Declined Payment'; member's status is moved to 'suspended'and the smart dunning recharge attempts start. 
mbr_tick_cancel_logic AS (   
        SELECT
              SK_TICKET
            , 'Payment Issue' AS CANCEL_ACTION
            , LAST_SK_DATE AS CANCEL_DATE
        FROM mbr_tick_last_event
        WHERE LAST_EVENT IN ('Missed Payment', 'Assumed Tokenization Issue', 'Declined Payment')
    UNION ALL    
-- SECOND SELECT - Identify members for cancellation when their last event is 'Refund' or 'Pending Cancellation' (member actively requesting to cancel).
        SELECT
              SK_TICKET
            , CASE
                WHEN LAST_EVENT = 'Refund' THEN 'Refund'
                WHEN LAST_EVENT = 'Pending Cancellation' THEN 'Cancel Requested'
              END AS CANCEL_ACTION
            , LAST_SK_DATE AS CANCEL_DATE
        FROM mbr_tick_last_event 
        WHERE LAST_EVENT IN ('Refund', 'Pending Cancellation')
    UNION ALL
-- THIRD SELECT - Identify action a member took (previous action) when their last event is 'Cancelled','Assumed Cancel', or'Default Cancelled'.
        SELECT
              l.SK_TICKET
            , CASE
                WHEN (p.MBR_TICK_STATUS = 0 OR p.PREV_EVENT = 'Pending Cancellation') THEN
                    CASE
                        WHEN p.PREV_EVENT = 'Pending Cancellation' THEN 'Cancel Requested'
                        WHEN p.PREV_EVENT IN ('Assumed Cancelled', 'Cancelled') THEN 'Cancelled'
                        WHEN p.PREV_EVENT IN ('Declined Payment', 'Missed Payment', 'Assumed Tokenization Issue') THEN 'Payment Issue'
                        WHEN p.PREV_EVENT = 'Refund' THEN 'Refund'
                        ELSE 'Cancelled'
                    END
                ELSE 'Cancel Requested'
              END AS CANCEL_ACTION
            , p.PREV_SK_DATE AS CANCEL_DATE
        FROM mbr_tick_last_event l
        LEFT JOIN mbr_tick_prev_event p
            USING (SK_TICKET)
        WHERE l.LAST_EVENT IN ('Cancelled', 'Assumed Cancelled', 'Default Cancelled')
    UNION ALL
-- FOURTH SELECT - Identify members whose last event is an upgrade event (parent ticket retired due to upgrade).
        SELECT
              SK_TICKET
            , 'Upgraded' AS CANCEL_ACTION
            , LAST_SK_DATE AS CANCEL_DATE
        FROM mbr_tick_last_event
        WHERE LAST_EVENT IN ('Upgraded From Active', 'Upgraded From Inactive')
),
--Secondary CTE for saftey to ensure only one cancel event is captured per member sk_ticket in the previous union CTE. No cases were found where multiple cancel events existed. 
--If multiple cancel events do exist for the same member sk_ticket, this logic will keep the most recent cancel event.
mbr_tick_cancel_event AS (
  SELECT 
    *
  FROM mbr_tick_cancel_logic
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SK_TICKET
    ORDER BY CANCEL_DATE DESC
  ) = 1
),
--For each member sk_ticket, identify the last event where rn = 1 and the event is 'Cancelled', 'Assumed Cancel', or 'Default Cancelled'.    
mbr_tick_term_event AS (
    SELECT 
          SK_TICKET
        , SK_DATE AS TERM_SK_DATE
    FROM mbr_tick_events
    WHERE rn = 1 
        AND EVENTTYPE IN ('Cancelled', 'Assumed Cancelled', 'Default Cancelled')
),
--For each member sk_ticket, identify if they are still active in Roller (active = 1; terminated = 0). If the member is still active then the status is 1 and term_date is null.
mbr_tick_active_status AS (
    SELECT
          l.SK_TICKET
        , l.LAST_EVENT
        , CASE
            WHEN l.MBR_TICK_STATUS = 0 THEN l.LAST_SK_DATE
            ELSE NULL
          END AS TERM_DATE
        , CASE
            WHEN l.MBR_TICK_STATUS = 0 THEN 0
            --Use COALESCE(LAST_RECURR_PAY_DATE, JOIN_SK_DATE) to catch members Roller still marks active but who have lapsed. Only applied to monthly recurring memberships. 
            --if today is 33+ days (dunning payment failure process begins) past the last recurring payment / member join date, the member is treated as inactive. 
            WHEN LOWER(t.RECURRINGPAYMENTFREQUENCY) = 'monthly'
             AND CURRENT_DATE >= DATEADD(DAY, 33, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE)) THEN 0
            ELSE 1
          END AS MBR_TICK_STATUS
    FROM mbr_tick_last_event l
    LEFT JOIN GOLD_DB.DW.DIMTICKET t
        ON l.SK_TICKET = t.SK_TICKET
    LEFT JOIN mbr_tick_recurring_dues rd
        ON l.SK_TICKET = rd.SK_TICKET
    LEFT JOIN mbr_tick_join_event j
        ON l.SK_TICKET = j.SK_TICKET
),
-- For each member sk_ticket, add the sk_location, sk_customer (purchasing customer), sk_booking, sk_discount, sk_employee and sk_product dimensions to the most recent record. 
mbr_tick_dim_rev AS (
    SELECT 
          SK_TICKET
        , SK_LOCATION
        , SK_CUSTOMER 
        , SK_BOOKING 
        , SK_DISCOUNT 
        , SK_BOOKINGCREATEDBYEMPLOYEE
        , SK_PAYMENTTAKENBYEMPLOYEE
        , SK_PRODUCT
        , BOOKINGITEMID
    FROM GOLD_DB.DW.FACTREVENUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY BOOKINGITEMID
        ORDER BY RECORDDATE DESC
    ) = 1   
),
-- Add actual attendance to the record. Track the total number of visits per member by sk_ticket, along with their most recent attendance date.
mbr_tick_attendance AS (
  SELECT
      SK_TICKET
    , SK_PURCHASINGCUSTOMER 
    , SK_JUMPERCUSTOMER
    , SK_HOUSEHOLD
    , SK_DATE_CHECKIN AS LAST_CHECKIN                                
    , COUNT(DISTINCT SK_DATE_CHECKIN)
        OVER (PARTITION BY SK_TICKET) AS ATTENDANCE_DAYS
  FROM GOLD_DB.DW.FACTATTENDANCE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SK_TICKET
    ORDER BY CHECKINDATETIME DESC
  ) = 1
)
SELECT DISTINCT --Distinct to ensure that ANY TICKETID is duplicated (duplicated memberships)
      dr.SK_LOCATION
    , dr.SK_PRODUCT
    , dr.SK_BOOKING
    , dr.SK_BOOKINGCREATEDBYEMPLOYEE
    , dr.SK_PAYMENTTAKENBYEMPLOYEE
    , j.JOIN_SK_DATE AS SK_JOIN_DATE
    , a.LAST_CHECKIN AS SK_LAST_CHECKIN
    , u.UPGRADE_DATE AS SK_UPGRADE_DATE
    , c.CANCEL_DATE AS SK_ATTRITION_DATE
    , r.MAX_REFUND_SK_DATE AS SK_LAST_REFUND_DATE
    --If a member ticket has a payment issue, refund, or upgrade the account is closed so use the cancel date from the cancel event table. 
    --If there is no payment issue but there is a cancel date from the term event table, then use that date. If there is no cancellation, then term_date is null.
    , CASE
        WHEN c.CANCEL_ACTION = 'Payment Issue' THEN c.CANCEL_DATE
        WHEN c.CANCEL_ACTION = 'Refund' THEN c.CANCEL_DATE
        WHEN c.CANCEL_ACTION = 'Upgraded' THEN c.CANCEL_DATE
        ELSE tm.TERM_SK_DATE
      END AS SK_TERMINATION_DATE 
    , dl.BUSINESSGROUP AS BUSINESS_GROUP
    , dl.LOCATIONID
    , t.TICKETID
    , SPLIT_PART(t.TICKETID, '-', 1) AS BOOKINGID
    , t.CUSTOMTICKETID
    , j.BOOKINGITEMID
    , db.BOOKINGLOCATIONSTANDARDIZED AS CONV_TYPE
    , act.LAST_EVENT AS LAST_STATUS
    , purch_dc.CUSTOMERID AS PURCH_CUSTOMER
    , jump_dc.CUSTOMERID AS JUMPER_CUSTOMER 
    , dp.PRODUCTID   AS PRODUCTID
    , dp.PRODUCTNAME AS PRODUCT_NAME
    , dp.OPERATIONSSUBGROUP AS SUB_GROUP --Changelog Daniel
    , ip.INITIAL_PAYMENT
    , rd.RECURR_AVG_DUES
    , rd.RECURR_PAY_COUNT
    , rd.LAST_RECURR_PAY_DATE
    , t.recurringpaymentfrequency AS pay_freq 
    , a.ATTENDANCE_DAYS
    , c.CANCEL_ACTION AS ATTRITION_REASON
    --The number of days between join and cancellation used for attrition and retention analysis. If there is no cancellation, then this value is null.
    , CASE 
        WHEN c.CANCEL_DATE IS NULL THEN NULL
        ELSE GREATEST(
            0,
            DATEDIFF(
                DAY,
                TO_DATE(j.JOIN_SK_DATE),
                TO_DATE(c.CANCEL_DATE)
            )
        )
      END AS ATTRITION_DAYS 
    , r.REFUND_REQUESTS
    --Override the member ticket active status if the ticket has a cancel, term, or upgrade event to indicate the member is no longer active. 1 = active; 0 = inactive. 
    --This metric is for future use to forecast coming attrition and potentially use to provide benefits to keep the member active.
    , CASE    
        WHEN c.CANCEL_ACTION IS NOT NULL THEN 0
        ELSE 1
      END AS PROJ_RETENTION_STATUS
    , act.MBR_TICK_STATUS AS ACTIVE_STATUS  
FROM GOLD_DB.DW.DIMTICKET t
INNER JOIN mbr_tick_join_event j  
    ON t.SK_TICKET = j.SK_TICKET
LEFT JOIN mbr_tick_initial_pay ip
    ON t.SK_TICKET = ip.SK_TICKET
LEFT JOIN mbr_tick_recurring_dues rd
    ON t.SK_TICKET = rd.SK_TICKET
LEFT JOIN mbr_tick_upgrade_event u
    ON t.SK_TICKET = u.SK_TICKET
LEFT JOIN mbr_tick_refund_event r
    ON t.SK_TICKET = r.SK_TICKET
LEFT JOIN mbr_tick_cancel_event c 
    ON t.SK_TICKET = c.SK_TICKET
LEFT JOIN mbr_tick_term_event tm
    ON t.SK_TICKET = tm.SK_TICKET
LEFT JOIN mbr_tick_dim_rev dr
    ON j.BOOKINGITEMID = dr.BOOKINGITEMID
LEFT JOIN mbr_tick_active_status act
    ON t.SK_TICKET = act.SK_TICKET
LEFT JOIN mbr_tick_attendance a
    ON t.SK_TICKET = a.SK_TICKET
LEFT JOIN GOLD_DB.DW.DIMPRODUCT dp
    ON dr.SK_PRODUCT = dp.SK_PRODUCT
LEFT JOIN GOLD_DB.DW.DIMLOCATION dl
    ON dr.SK_LOCATION = dl.SK_LOCATION
LEFT JOIN GOLD_DB.DW.DIMBOOKING db
    ON dr.SK_BOOKING = db.SK_BOOKING
-- FactRev sometimes has null sk_customer, so attendance is used as a backup to join to the customer dimension. ONCE THIS BUG IS SOLVED WE CAN REMOVE THE COLAESCE CLAUSE
LEFT JOIN GOLD_DB.DW.DIMCUSTOMER purch_dc
    ON COALESCE(NULLIF(dr.SK_CUSTOMER, -1), a.SK_PURCHASINGCUSTOMER) = purch_dc.SK_CUSTOMER 
LEFT JOIN GOLD_DB.DW.DIMCUSTOMER jump_dc
    ON a.SK_JUMPERCUSTOMER = jump_dc.SK_CUSTOMER
WHERE LOWER(t.RECURRINGPAYMENTFREQUENCY) = 'monthly'
AND LOCATIONID = 'Apex, NC - 151'
AND   LOWER(dp.operationssubgroup) NOT IN ('annual')
AND   LOWER(dp.productname) LIKE '%member%' --Removed 358 TICKETIDs associated to products that are NOT a Membership
AND   LOWER(dp.productname) NOT LIKE '%membership activation fee'
ORDER BY j.JOIN_SK_DATE DESC, dl.LOCATIONID ASC
;