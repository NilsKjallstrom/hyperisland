# hyperisland
HyperIslandAssessments
SQL Query for retrieving data from big query and upload to tableau:
SELECT
  region,
  DATE,
  landing_page,
  SUM(screen_page_views) AS screen_page_views,
  COUNT(DISTINCT session_id) AS sessions,
  SUM(CAST(engaged_sessions AS INT)) AS engaged_sessions,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  COUNT(DISTINCT new_users) AS new_users,
  SAFE_DIVIDE(
    SUM(engagement_time_seconds),
    COUNT(DISTINCT user_pseudo_id)
  ) AS average_engagement_time_per_user
FROM
  (
    SELECT
      geo.region AS region,
      event_date AS DATE,
      MAX(
        (
          SELECT
            value.string_value
          FROM
            UNNEST (event_params)
          WHERE
            event_name = 'session_start'
            AND key = 'page_location'
        )
      ) AS landing_page,
      COUNT(
        CASE
          WHEN event_name = 'page_view' THEN 1
        END
      ) AS screen_page_views,
      CONCAT(
        user_pseudo_id,
        (
          SELECT
            value.int_value
          FROM
            UNNEST (event_params)
          WHERE
            key = 'ga_session_id'
        )
      ) AS session_id,
      MAX(
        (
          SELECT
            value.string_value
          FROM
            UNNEST (event_params)
          WHERE
            key = 'session_engaged'
        )
      ) AS engaged_sessions,
      user_pseudo_id,
      MAX(
        CASE
          WHEN event_name = 'first_visit' THEN user_pseudo_id
          ELSE NULL
        END
      ) AS new_users,
      SAFE_DIVIDE(
        SUM(
          (
            SELECT
              value.int_value
            FROM
              UNNEST (event_params)
            WHERE
              key = 'engagement_time_msec'
          )
        ),
        1000
      ) AS engagement_time_seconds
    FROM
      `individualproject-398014.analytics_401142256.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20230903' AND '20230914'
    GROUP BY
      region,
      DATE,
      session_id,
      user_pseudo_id
  )
GROUP BY
  region,
  DATE,
  landing_page
ORDER BY
  region DESC
