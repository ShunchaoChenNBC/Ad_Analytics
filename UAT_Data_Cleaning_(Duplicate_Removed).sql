DECLARE updt DATE DEFAULT '2022-10-28'; --hard-coded cue point date

--no need to run anymore, just having for Table referencing
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.ad_exp_cue_point_summary_no_duplicates` as
with tbl as (
SELECT 

  CASE WHEN seriesTitle IS NULL THEN assetName ELSE seriesTitle END AS Video_Series_Name,

  a.assetExternalID,
--  CASE WHEN AssetName = "NULL" THEN NULL ELSE REGEXP_REPLACE(Asset_Name, 'Peacock: ', '') END AS Asset_Name, 
  assetName,
--  CASE WHEN Asset_Duration = "NULL" THEN NULL ELSE Asset_Duration END AS Asset_Duration, 
  assetDuration,
--  CASE WHEN Asset_Duration = "NULL" THEN NULL ELSE SAFE_CAST(Asset_Duration AS FLOAT64)/60 END AS Asset_Duration_minutes, 
    SAFE_CAST(assetDuration AS FLOAT64)/60 AS Asset_Duration_minutes,
  createdAt,
  agingDate,
--  CASE WHEN total_number_of_cue_points = "NULL" THEN NULL ELSE total_number_of_cue_points END AS total_number_of_cue_points, 
  cuePointLength,
--  CASE WHEN a.cue_point_sequence = "NULL" THEN NULL ELSE a.cue_point_sequence END AS cue_point_sequence,
  a.cuePointPosition,
--  CASE WHEN contentTimePosition = "NULL" THEN NULL ELSE contentTimePosition END AS contentTimePosition,
  contentTimePosition,
  SAFE_CAST(contentTimePosition AS FLOAT64)/60 AS Content_Breaks,
  SAFE_CAST(contentTimePosition AS FLOAT64)/SAFE_CAST(assetDuration AS FLOAT64) AS Content_Breaks_percent, --SAFE_DIVIDE?
  --custom cue point categorizations
    CASE WHEN cuePointLength IS NULL THEN "NULL"
         WHEN cuePointLength = a.cuePointLength THEN "END"
         ELSE "MID"
         END AS ad_cue,
    CASE WHEN cuePointLength IS NULL THEN SAFE_CAST(assetDuration AS FLOAT64) --NULL NP
         WHEN cuePointLength = a.cuePointLength THEN (SAFE_CAST(assetDuration AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/60 
            ELSE (SAFE_CAST(next_break AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/60
         END AS Content_Segments,
    CASE WHEN cuePointLength IS NULL THEN SAFE_CAST(assetDuration AS FLOAT64) --NULL NP
         WHEN cuePointLength = a.cuePointLength THEN (SAFE_CAST(assetDuration AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/SAFE_CAST(assetDuration AS FLOAT64)
            ELSE ((SAFE_CAST(next_break AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/SAFE_CAST(assetDuration AS FLOAT64))
         END AS Content_Segments_percent,
    CASE WHEN assetDuration IS NULL THEN NULL
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 THEN "< 10"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 THEN "10-19"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 THEN "20-39"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 THEN "40-59"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 THEN "60-89"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 THEN "90-119"
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 THEN "120-149"
         ELSE "150+"
         END AS duration,
  --compass data
    Primary_Genre,
    Secondary_Genre,
    ProductType,
    b.SeasonNumber,
    b.EpisodeNumber,
    TypeOfContent,
    Distributor,
    CoppaCompliance,
    adRequirementsOnAVOD,
    adRequirementsOnPremiumTier,
    adRequirementsOnPremiumPlusTier,
    Rev_Share,
  --ad grade prep
    CASE WHEN assetDuration IS NULL THEN 0 -- NP
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 THEN 0
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 THEN 2
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 THEN 3
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 THEN 5
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 THEN 6
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 THEN 10
         ELSE 13
         END AS ad_spec
FROM  `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelCuepointView`   a
LEFT JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_COMPASS_METADATA` b 
    ON LOWER(a.assetExternalID) = LOWER(b.ContentID)
LEFT JOIN (SELECT assetExternalID, cuePointPosition, contentTimePosition as next_break
           FROM  `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelCuepointView`  ) c 
    ON a.assetExternalID = c.assetExternalID AND SAFE_CAST(a.cuePointPosition AS INT64) = SAFE_CAST(c.cuePointPosition AS INT64)-1
),
tbl2 as (
SELECT a.*, 
  --ad grade
    CASE WHEN a.cuePointLength IS NULL AND a.assetDuration IS NULL   THEN "NULL"
         WHEN a.cuePointLength IS NULL AND a.ad_spec = 0               THEN "At Spec"
         WHEN a.cuePointLength IS NULL                               THEN "Below Spec"
         WHEN SAFE_CAST(a.cuePointLength AS INT64) > a.ad_spec          THEN "Above Spec"
         WHEN SAFE_CAST(a.cuePointLength AS INT64) = a.ad_spec          THEN "At Spec"
         WHEN SAFE_CAST(a.cuePointLength AS INT64) < a.ad_spec          THEN "Below Spec"
         END AS ad_grade
      , CAST(b.ad_spec AS DECIMAL) / CAST(b.cuePointLength AS DECIMAL) as Multiplier
      , (CAST(b.cuePointLength AS DECIMAL) + 1) / CAST(b.cuePointLength AS DECIMAL) as Multiplier_just_one_more
      , c.Content_Segments_MAX, c.Content_Segments_MAX / 2 AS Content_Segments_MAX_split
FROM tbl a
  LEFT JOIN tbl b on a.assetExternalID = b.assetExternalID AND a.cuePointLength = b.cuePointPosition
  LEFT JOIN (select assetExternalID, MAX(Content_Segments) AS Content_Segments_MAX from tbl GROUP BY assetExternalID) c on a.assetExternalID = c.assetExternalID
WHERE lower(a.assetName) NOT LIKE lower('%do%not%use%') AND lower(a.distributor) NOT LIKE lower('%nbc%test%')
ORDER BY a.Video_Series_Name, CAST(a.SeasonNumber AS DECIMAL), CAST(a.EpisodeNumber AS DECIMAL)
  , a.assetName, CAST(a.cuePointPosition AS DECIMAL), assetName
)

select *, 
updt as updated_date
from tbl2
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35

