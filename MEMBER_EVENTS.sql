WITH
--Sort member events in descending order (newest first). Use this to identify member actioned cancellation events that occurred prior to termination. 
--BUG FIX: Added MAX recurring and upgraded date to account for recurring payments bug 
mbr_tick_events AS (
    SELECT
          fm.SK_TICKET
        , fm.BOOKINGITEMID
        , fm.SK_EVENTTYPE
        , dm.EVENTTYPE
        , fm.SK_DATE
        , fm.SK_TIME
        , TIMESTAMP_NTZ_FROM_PARTS(fm.SK_DATE, fm.SK_TIME) AS SK_DATETIME
        , fm.VALUE
        , fm.FLAG_REFUNDED
        , dm.ACTIVE
        , dm.SORTORDER2
        , MAX(CASE WHEN dm.EVENTTYPE = 'Recurring Payment'
                   THEN fm.SK_DATE END)
              OVER (PARTITION BY fm.SK_TICKET) AS LAST_RECURR_PAY_DATE
        , MAX(CASE WHEN dm.EVENTTYPE IN ('Upgraded From Active', 'Upgraded From Inactive')
                   THEN fm.SK_DATE END)
              OVER (PARTITION BY fm.SK_TICKET) AS LAST_UPGRADE_DATE
    FROM GOLD_DB.DW.FACTMEMBERSHIPPASSEVENTS fm
    JOIN GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING(SK_EVENTTYPE)
),
--BUG FIX: Filter out events that occurred after the effective cancel (cancel with no subsequent recurring payment or upgrade).
--Tickets with no effective cancel pass through unchanged based on 'EFFECTIVE_CANCEL_DATETIME IS NULL'
mbr_tick_events_subset AS (
    SELECT *
    FROM (
        SELECT
              *
            , MAX(CASE
                    WHEN EVENTTYPE IN ('Cancelled', 'Assumed Cancelled', 'Default Cancelled')
                     AND (LAST_RECURR_PAY_DATE IS NULL OR LAST_RECURR_PAY_DATE < SK_DATE)
                     AND (LAST_UPGRADE_DATE IS NULL OR LAST_UPGRADE_DATE < SK_DATE)
                    THEN SK_DATETIME
                  END) OVER (PARTITION BY SK_TICKET) AS EFFECTIVE_CANCEL_DATETIME
        FROM mbr_tick_events
    )
    WHERE EFFECTIVE_CANCEL_DATETIME IS NULL
       OR SK_DATETIME <= EFFECTIVE_CANCEL_DATETIME
),
--BUG FIX: Cleaned member sk_ticket events excluding events that occurred after effective cancel. All other CTEs use this clean view for rn-based lookups.
mbr_tick_events_clean AS (
    SELECT
          SK_TICKET
        , BOOKINGITEMID
        , SK_EVENTTYPE
        , EVENTTYPE
        , SK_DATE
        , SK_TIME
        , SK_DATETIME
        , VALUE
        , FLAG_REFUNDED
        , ACTIVE
        , SORTORDER2
        , ROW_NUMBER() OVER (
            PARTITION BY SK_TICKET
            ORDER BY SK_DATETIME DESC, SORTORDER2 DESC
          ) AS rn
    FROM mbr_tick_events_subset
),
--For each member sk_ticket, retain only the most recent effective event (rn = 1).
mbr_tick_last_event AS (
    SELECT
          SK_TICKET
        , ACTIVE AS MBR_TICK_STATUS
        , SK_EVENTTYPE AS LAST_SK_EVENT
        , EVENTTYPE AS LAST_EVENT
        , SK_DATE AS LAST_SK_DATE
    FROM mbr_tick_events_clean
    WHERE rn = 1
),
--For each member sk_ticket, capture the event immediately preceding the most recent effective event (where rn = 2).
mbr_tick_prev_event AS (
    SELECT 
          SK_TICKET
        , ACTIVE AS MBR_TICK_STATUS
        , SK_EVENTTYPE AS PREV_SK_EVENT 
        , EVENTTYPE AS PREV_EVENT
        , SK_DATE AS PREV_SK_DATE
    FROM mbr_tick_events_clean
    WHERE rn = 2 
),
--For each member sk_ticket, identify the create date as the join date. Use Qualify to guarantee there are not multiple joins.
mbr_tick_join_event AS (
    SELECT
          SK_TICKET
        , BOOKINGITEMID
        , SK_DATE AS JOIN_SK_DATE
    FROM mbr_tick_events_clean
    WHERE EVENTTYPE = 'Creation'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SK_TICKET
        ORDER BY SK_DATETIME ASC
    ) = 1
),
--For each member sk_ticket, identify the inital payment dues. Use Qualify to guarantee there are not multiple initial payments. Creation and initial payment occur at the same time so it doesnt matter which event we pull.
mbr_tick_initial_pay AS (
    SELECT
          SK_TICKET
        , ROUND(VALUE, 2) AS PAY_INITIAL
    FROM mbr_tick_events_clean
    WHERE EVENTTYPE = 'Initial Payment'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SK_TICKET
        ORDER BY SK_DATETIME ASC
    ) = 1
),
--For each member sk_ticket, identify average recurring payment dues.
mbr_tick_recurring_dues AS (
    SELECT
          SK_TICKET
        , COUNT(DISTINCT DATE(SK_DATE)) AS RECURR_PAY_COUNT
        , ROUND(AVG(VALUE), 2) AS RECURR_AVG_DUES
        , MAX(SK_DATE) AS LAST_RECURR_PAY_DATE
    FROM mbr_tick_events_clean
    WHERE EVENTTYPE = 'Recurring Payment'
    GROUP BY SK_TICKET
),
--Track only the most recent upgrade event per ticket. Capture the upgrade on the parent sk_ticket only; the associated child sk_ticket should not reflect a separate upgrade event.
--Use rn = 1 to ensure the upgrade is the ticket's current state — tickets with a buried upgrade event followed by continued activity (e.g. recurring payments) are genuinely active and should not be flagged.
mbr_tick_upgrade_event AS (
    SELECT
          SK_TICKET
        , SK_DATE AS UPGRADE_DATE
    FROM mbr_tick_events_clean
    WHERE rn = 1
        AND EVENTTYPE IN ('Upgraded From Active', 'Upgraded From Inactive')
),
--For each member sk_ticket, find the max refund request date and number of times a member has requested a refund
mbr_tick_refund_event AS (
    SELECT
          SK_TICKET
        , MAX(SK_DATE) AS MAX_REFUND_SK_DATE
        , COUNT(DISTINCT SK_DATE) AS REFUND_COUNT
    FROM mbr_tick_events_clean
    WHERE EVENTTYPE = 'Refund'
    GROUP BY SK_TICKET
),
-- CTE - Capture the cancel reason and the effective cancel date. Note that memberships may remain active through the end of the monthly term.
-- FIRST SELECT - Last event is a payment failure. Tickets with a cancel event and no subsequent Recurring Payment
-- or Upgrade are handled by THIRD SELECT (mbr_tick_last_event promotes the cancel to last event for those tickets).
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
                        WHEN p.PREV_EVENT = 'Assumed Cancelled' THEN 'Cancel Assumed'
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
    UNION ALL
-- FIFTH SELECT - Identify monthly members Roller still marks active but 33+ days past their last recurring payment or join date.
-- These are treated as lapsed and given a computed term date (last payment/join date + 33 days).
-- BUG FIX: may reflect missing cancellation events in source data rather than true lapsed memberships; filter CANCEL_REASON = 'Lapsed' to exclude when upstream data is corrected.
        SELECT
              l.SK_TICKET
            , 'Lapsed' AS CANCEL_ACTION
            , DATEADD(DAY, 33, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE)) AS CANCEL_DATE
        FROM mbr_tick_last_event l
        LEFT JOIN mbr_tick_recurring_dues rd
            ON l.SK_TICKET = rd.SK_TICKET
        LEFT JOIN mbr_tick_join_event j
            ON l.SK_TICKET = j.SK_TICKET
        WHERE l.MBR_TICK_STATUS = 1
            AND CURRENT_DATE >= DATEADD(DAY, 33, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE))
),
--Secondary CTE for safety to ensure only one cancel event is captured per member sk_ticket.
--Orders by non-Lapsed first so a real cancel event always returns over a computed lapsed date, then by most recent cancel date.
mbr_tick_cancel_event AS (
  SELECT
    *
  FROM mbr_tick_cancel_logic
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SK_TICKET
    ORDER BY CASE WHEN CANCEL_ACTION = 'Lapsed' THEN 1 ELSE 0 END ASC, CANCEL_DATE DESC
  ) = 1
),
--For each member sk_ticket, identify the cancellation date as the term date.
--Uses mbr_tick_last_event which already accounts for cancel-always-wins logic and the Pending Cancellation sort bug.
mbr_tick_term_event AS (
    SELECT
          SK_TICKET
        , LAST_SK_DATE AS TERM_SK_DATE
    FROM mbr_tick_last_event
    WHERE LAST_EVENT IN ('Cancelled', 'Assumed Cancelled', 'Default Cancelled')
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
--For each member sk_ticket, add the sk_location, sk_customer (purchasing customer), sk_booking, sk_discount, sk_employee and sk_product dimensions to the most recent record. 
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
--For each member sk_ticket forecast the next recurring payment date. Anchors to the last payment day (Roller locks billing to this day), falling back to join day for new members.
--Clamped to the last day of the next month to handle short months (e.g. Jan 31 → Feb 28).
--Note: Roller clamps recurring payments to the last day of the month and locks recurring to that day going forward (e.g. joined Jan 31, clamp to Feb 28, now recurr will be 28th every month thereafter).
mbr_tick_next_payment AS (
    SELECT
          l.SK_TICKET
        , DATE_FROM_PARTS(
            YEAR(DATEADD(MONTH, 1, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE))),
            MONTH(DATEADD(MONTH, 1, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE))),
            LEAST(
                DAY(COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE)),
                DAY(LAST_DAY(DATEADD(MONTH, 1, COALESCE(rd.LAST_RECURR_PAY_DATE, j.JOIN_SK_DATE))))
            )
          ) AS NEXT_RECURRING_PAYMENT_DATE
    FROM mbr_tick_last_event l
    JOIN mbr_tick_join_event j
        ON l.SK_TICKET = j.SK_TICKET
    LEFT JOIN mbr_tick_recurring_dues rd
        ON l.SK_TICKET = rd.SK_TICKET
    JOIN GOLD_DB.DW.DIMTICKET t
        ON l.SK_TICKET = t.SK_TICKET
    WHERE LOWER(t.RECURRINGPAYMENTFREQUENCY) = 'monthly'
),
--Add actual attendance to the record. Track the total number of visits per member by sk_ticket, along with their most recent attendance date.
mbr_tick_attendance AS (
  SELECT
      SK_TICKET
    , SK_PURCHASINGCUSTOMER 
    , SK_JUMPERCUSTOMER
    , SK_HOUSEHOLD
    , SK_DATE_CHECKIN AS LAST_CHECKIN                                
    , COUNT(DISTINCT SK_DATE_CHECKIN)
        OVER (PARTITION BY SK_TICKET) AS CHECKIN_COUNT
  FROM GOLD_DB.DW.FACTATTENDANCE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SK_TICKET
    ORDER BY CHECKINDATETIME DESC
  ) = 1
),
--BUG FIX: For each member ticket, capture the current membership status from Roller's status bridge table. Takes the most recent NEWSTATUS as the last known status.
mbr_tick_roller_status AS (
    SELECT
          TICKETID
        , NEWSTATUS       AS ROLLER_STATUS
        , DATE(EVENTDATE) AS ROLLER_STATUS_DATE
    FROM SILVER_DB.DWELT.BRIDGEMEMBERSHIP_STATUSES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TICKETID
        ORDER BY EVENTDATE DESC
    ) = 1
)
SELECT DISTINCT --Distinct to ensure that ANY TICKETID is duplicated (duplicated memberships)
      dr.SK_LOCATION
    , dr.SK_PRODUCT
    , dr.SK_BOOKING
    , t.SK_TICKET
    , dr.SK_BOOKINGCREATEDBYEMPLOYEE
    , a.SK_HOUSEHOLD
    , a.SK_PURCHASINGCUSTOMER
    , a.SK_JUMPERCUSTOMER
    , j.JOIN_SK_DATE AS DATE_JOIN
    , a.LAST_CHECKIN AS DATE_LAST_CHECKIN
    , u.UPGRADE_DATE AS DATE_UPGRADE
    --BUG FIX: CANCEL_DATE from cancel logic may be null for roller-terminated members missing a cancel event; fall back to roller status date.
    , r.MAX_REFUND_SK_DATE AS DATE_LAST_REFUND
    --If a member ticket has a payment issue, refund, or upgrade the account is closed so use the cancel date from the cancel event table. 
    --If there is no payment issue but there is a cancel date from the term event table, then use that date. If there is no cancellation, then term_date is null.
    , COALESCE(c.CANCEL_DATE, CASE WHEN rs.ROLLER_STATUS IN ('Terminated', 'Upgraded') THEN rs.ROLLER_STATUS_DATE END) AS DATE_CANCEL
    , CASE
        WHEN c.CANCEL_ACTION IN ('Payment Issue', 'Refund', 'Upgraded', 'Lapsed') THEN c.CANCEL_DATE
        --BUG FIX: Override the termination date due to the known recurring payment bug if the last Roller status is 'Terminated' or 'Upgraded'
        WHEN rs.ROLLER_STATUS IN ('Terminated', 'Upgraded') THEN rs.ROLLER_STATUS_DATE
        ELSE tm.TERM_SK_DATE
      END AS DATE_TERMINATION
    , dl.BUSINESSGROUP AS BUSINESS_GROUP
    , dl.LOCATIONID
    , t.TICKETID
    , SPLIT_PART(t.TICKETID, '-', 1) AS BOOKINGID
    , t.CUSTOMTICKETID
    , j.BOOKINGITEMID
    , db.BOOKINGLOCATIONSTANDARDIZED AS CONV_TYPE
    , act.LAST_EVENT AS STATUS_LAST
    --Bug Fix: Included to override the FACTMEMBERSHIPPASSEVENTS status / active issues
    , rs.ROLLER_STATUS AS STATUS_ROLLER
    , purch_dc.CUSTOMERID AS CUSTOMER_PURCHASE
    , jump_dc.CUSTOMERID AS CUSTOMER_JUMPER
    , dp.PRODUCTNAME AS PRODUCT_NAME
    --, dp.OPERATIONSSUBGROUP AS SUB_GROUP 
    , a.CHECKIN_COUNT
    , t.RECURRINGPAYMENTFREQUENCY AS PAY_FREQ
    , ip.PAY_INITIAL
    , rd.RECURR_AVG_DUES
    , rd.RECURR_PAY_COUNT
    , rd.LAST_RECURR_PAY_DATE AS RECURR_LAST_PAY_DATE 
    , CASE
        WHEN c.CANCEL_ACTION IS NULL THEN np.NEXT_RECURRING_PAYMENT_DATE
      END AS RECURR_NEXT_PAY_DATE
    , r.REFUND_COUNT
    --BUG FIX: Override the cancel reason due to the known recurring payment bug if the last Roller status is 'Terminated' or 'Upgraded'
    , CASE
        WHEN c.CANCEL_ACTION IS NOT NULL THEN c.CANCEL_ACTION
        WHEN rs.ROLLER_STATUS = 'Terminated' THEN 'Term Roller'
        WHEN rs.ROLLER_STATUS = 'Upgraded' THEN 'Upgraded'
        ELSE NULL
      END AS CANCEL_REASON
    --The number of days between join and cancellation used for cancel and retention analysis. If there is no cancellation, then this value is null.
    --BUG FIX: CANCEL_DAYS was null for roller-terminated/upgraded members missing a cancel event; use roller status date as fallback cancel anchor.
    , CASE
        WHEN c.CANCEL_DATE IS NULL AND NOT (rs.ROLLER_STATUS IN ('Terminated', 'Upgraded')) THEN NULL
        ELSE GREATEST(
            0,
            DATEDIFF(
                DAY,
                TO_DATE(j.JOIN_SK_DATE),
                TO_DATE(COALESCE(c.CANCEL_DATE, rs.ROLLER_STATUS_DATE))
            )
        )
      END AS CANCEL_DAYS
    --Override the member ticket active status if the ticket has a cancel, term, or upgrade event to indicate the member is no longer active. 1 = active; 0 = inactive. 
    --This metric is for future use to forecast coming churn and potentially use to provide benefits to keep the member active.
    --BUG FIX: Members terminated in Roller without a matching cancel event would incorrectly show as retained; treat Roller 'Terminated' as inactive.
    , CASE
        WHEN c.CANCEL_ACTION IS NOT NULL THEN 0
        WHEN rs.ROLLER_STATUS IN ('Terminated', 'Upgraded') THEN 0
        ELSE 1
      END AS STATUS_PROJ
    --BUG FIX: act.MBR_TICK_STATUS is derived from FACTMEMBERSHIPPASSEVENTS which may be missing termination/upgrade events; use Roller status as override.
    , CASE
        WHEN act.MBR_TICK_STATUS = 0 THEN 0
        WHEN rs.ROLLER_STATUS IN ('Terminated', 'Upgraded') THEN 0
        ELSE act.MBR_TICK_STATUS
      END AS STATUS_ACTIVE
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
LEFT JOIN mbr_tick_next_payment np
    ON t.SK_TICKET = np.SK_TICKET
LEFT JOIN mbr_tick_attendance a
    ON t.SK_TICKET = a.SK_TICKET
LEFT JOIN GOLD_DB.DW.DIMPRODUCT dp
    ON dr.SK_PRODUCT = dp.SK_PRODUCT
LEFT JOIN GOLD_DB.DW.DIMLOCATION dl
    ON dr.SK_LOCATION = dl.SK_LOCATION
LEFT JOIN GOLD_DB.DW.DIMBOOKING db
    ON dr.SK_BOOKING = db.SK_BOOKING
--BUG FIX: FactRev sometimes has null sk_customer, so attendance is used as a backup to join to the customer dimension.
LEFT JOIN GOLD_DB.DW.DIMCUSTOMER purch_dc
    ON COALESCE(NULLIF(dr.SK_CUSTOMER, -1), a.SK_PURCHASINGCUSTOMER) = purch_dc.SK_CUSTOMER 
--BUG FIX: FACTMEMBERSHIPPASSEVENTS doesnt accurately capture the last status; using Roller silver layer bridge to get the last status
LEFT JOIN mbr_tick_roller_status rs 
    ON t.TICKETID = rs.TICKETID
LEFT JOIN GOLD_DB.DW.DIMCUSTOMER jump_dc
    ON a.SK_JUMPERCUSTOMER = jump_dc.SK_CUSTOMER
WHERE LOWER(t.RECURRINGPAYMENTFREQUENCY) = 'monthly'
AND   LOWER(dp.operationssubgroup) NOT IN ('annual')
AND   LOWER(dp.productname) LIKE '%member%' --Removed 358 TICKETIDs associated to products that are NOT a Membership
AND   LOWER(dp.productname) NOT LIKE '%membership activation fee'
ORDER BY j.JOIN_SK_DATE DESC, dl.LOCATIONID ASC
;