

with UAT as (
select *
from `nbcu-sdp-prod-003.sdp_persistent_views.FreewheelCuepointView`
where EXTRACT(YEAR FROM effectiveTo) = 9999 --- Only select the latest records
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35 -- remove duplicates
),

UAT1 as (
select 
seriesTitle,
assetExternalID,
assetName,
assetDuration,
genre,
createdAt,
agingDate,
contentTimePosition,
cuePointPosition,
cuePointLength,
seasonOrLibrary,
dayPart,
entitlement,
case when (Episode is not null or cast(Episode as string) != "") then Episode else episodeNumber end as episodeNumber,
case when (Season is not null or cast(Season as string) != "") then Season else seasonNumber end as seasonNumber,
fullEpisode,
language,
promo,
programmeType,
effectiveFrom,
effectiveTo,
sdpDIFTimestamp,
sdpSourceTimestamp,
sdpBusinessDate,
sdpETLTimestamp,
sdpSourceSystemName,
sdpSourceTransport,
sdpSourceOrigin,
sdpSource,
sdpSourceType,
sdpSourceTerritory,
sdpSourceProvider,
sdpSourceProposition,
SDPSnapshotUpdateTimestamp,
dense_rank() over (partition by seriesTitle, SeasonNumber, EpisodeNumber order by SDPSnapshotUpdateTimestamp desc) as  rk
from UAT u
left join `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Columbo_Mislabeled` m on lower(m.Video_Series_Name) = lower(u.seriesTitle) and lower(m.Asset_Name) = lower(u.assetName)
),


tbl as (
SELECT 

  CASE WHEN seriesTitle IS NULL THEN lower(assetName) ELSE lower(seriesTitle) END AS Video_Series_Name, -- standardized the format to lower case

LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(a.assetExternalID,'_UHDDV',''),'_HDSDR',''),'_UHDSDR',''),'_UHDHDR','')) as assetExternalID,
--  CASE WHEN AssetName = "NULL" THEN NULL ELSE REGEXP_REPLACE(Asset_Name, 'Peacock: ', '') END AS Asset_Name, 
  a.assetName,
--  CASE WHEN Asset_Duration = "NULL" THEN NULL ELSE Asset_Duration END AS Asset_Duration, 
  a.assetDuration,
--  CASE WHEN Asset_Duration = "NULL" THEN NULL ELSE SAFE_CAST(Asset_Duration AS FLOAT64)/60 END AS Asset_Duration_minutes, 
    round(SAFE_CAST(assetDuration AS FLOAT64)/60,2) AS Asset_Duration_minutes,
  createdAt,
  agingDate,
--  CASE WHEN total_number_of_cue_points = "NULL" THEN NULL ELSE total_number_of_cue_points END AS total_number_of_cue_points, 
 ifnull(a.cuePointLength,0) as cuePointLength,
--  CASE WHEN a.cue_point_sequence = "NULL" THEN NULL ELSE a.cue_point_sequence END AS cue_point_sequence,
  ifnull(a.cuePointPosition,0) as cuePointPosition,
--  CASE WHEN contentTimePosition = "NULL" THEN NULL ELSE contentTimePosition END AS contentTimePosition,
  ifnull(a.contentTimePosition,0) as contentTimePosition,
  ifnull(SAFE_CAST(a.contentTimePosition AS FLOAT64)/60,0) AS Content_Breaks,
  ifnull(SAFE_CAST(a.contentTimePosition AS FLOAT64)/SAFE_CAST(a.assetDuration AS FLOAT64),0) AS Content_Breaks_percent, --SAFE_DIVIDE?
  --custom cue point categorizations
    CASE WHEN a.cuePointLength IS NULL THEN "NULL"
         WHEN a.cuePointLength = a.cuePointLength THEN "END"
         ELSE "MID"
         END AS ad_cue,
    CASE WHEN a.cuePointLength IS NULL THEN SAFE_CAST(a.assetDuration AS FLOAT64) --NULL NP
         WHEN a.cuePointLength = a.cuePointLength THEN (SAFE_CAST(a.assetDuration AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/60 
            ELSE (SAFE_CAST(next_break AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/60
         END AS Content_Segments,
    CASE WHEN a.cuePointLength IS NULL THEN SAFE_CAST(assetDuration AS FLOAT64) --NULL NP
         WHEN a.cuePointLength = a.cuePointLength THEN (SAFE_CAST(assetDuration AS FLOAT64) - SAFE_CAST(contentTimePosition AS FLOAT64))/SAFE_CAST(assetDuration AS FLOAT64)
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
    ifnull(a.seasonNumber,0) as SeasonNumber,
    ifnull(a.episodeNumber,0) as EpisodeNumber, --- Use UAT seasons & episodes instead of S&E in Compass
    Case when lower(TypeOfContent) like "%d2c%" then "Peacock Original" else "Others" end as TypeOfContent, -- Simply type of content to 2 types
    Distributor,
    CoppaCompliance,
    adRequirementsOnAVOD,
    adRequirementsOnPremiumTier,
    adRequirementsOnPremiumPlusTier,
    Rev_Share,
  --ad grade prep
    CASE WHEN assetDuration IS NULL THEN 0 -- NP
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 and b.Primary_Genre = "Movies" THEN 0
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 and b.Primary_Genre = "Movies" THEN 2
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 and b.Primary_Genre = "Movies" THEN 3
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 and b.Primary_Genre = "Movies" THEN 5
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 and b.Primary_Genre = "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 and b.Primary_Genre = "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 and b.Primary_Genre = "Movies" THEN 10
        WHEN SAFE_CAST(assetDuration AS INT64)/60 < 180 and b.Primary_Genre = "Movies" THEN 13
        WHEN SAFE_CAST(assetDuration AS INT64)/60 >= 180 and b.Primary_Genre = "Movies" THEN 15 -- add additional bracket to separate Movie and other (TV) bracklets
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 and b.Primary_Genre != "Movies" THEN 0
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 and b.Primary_Genre != "Movies" THEN 2
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 and b.Primary_Genre != "Movies" THEN 3
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 and b.Primary_Genre != "Movies" THEN 5
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 and b.Primary_Genre != "Movies" THEN 6
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 and b.Primary_Genre != "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 and b.Primary_Genre != "Movies" THEN 10
         ELSE 13
         END AS ad_spec
FROM UAT1 a
LEFT JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_COMPASS_METADATA` b 
    ON LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(a.assetExternalID,'_UHDDV',''),'_HDSDR',''),'_UHDSDR',''),'_UHDHDR','')) = LOWER(b.ContentID)
LEFT JOIN (SELECT LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(assetExternalID,'_UHDDV',''),'_HDSDR',''),'_UHDSDR',''),'_UHDHDR','')) as assetExternalID, 
cuePointPosition, contentTimePosition as next_break
           FROM  UAT) c 
    ON a.assetExternalID = c.assetExternalID AND SAFE_CAST(a.cuePointPosition AS INT64) = SAFE_CAST(c.cuePointPosition AS INT64)-1
where rk = 1 -- select the latest timestamp
),
tbl2 as (
SELECT a1.*, 
  --ad grade
    CASE WHEN a1.cuePointLength IS NULL AND a1.assetDuration IS NULL   THEN "NULL"
         WHEN a1.cuePointLength IS NULL AND a1.ad_spec = 0               THEN "At Spec"
         WHEN a1.cuePointLength IS NULL                               THEN "Below Spec"
         WHEN SAFE_CAST(a1.cuePointLength AS INT64) > a1.ad_spec          THEN "Above Spec"
         WHEN SAFE_CAST(a1.cuePointLength AS INT64) = a1.ad_spec          THEN "At Spec"
         WHEN SAFE_CAST(a1.cuePointLength AS INT64) < a1.ad_spec          THEN "Below Spec"
         END AS ad_grade
      , safe_divide(CAST(b1.ad_spec AS DECIMAL), CAST(b1.cuePointLength AS DECIMAL)) as Mutiplier -- solve 0/0 issue
      , safe_divide(CAST(b1.cuePointLength AS DECIMAL) + 1, CAST(b1.cuePointLength AS DECIMAL)) as Multiplier_just_one_more
      , c1.Content_Segments_MAX
      , c1.Content_Segments_MAX / 2 AS Content_Segments_MAX_split
FROM tbl a1
  LEFT JOIN tbl b1 on a1.assetExternalID = b1.assetExternalID AND a1.cuePointLength = b1.cuePointPosition
  LEFT JOIN (select assetExternalID, MAX(Content_Segments) AS Content_Segments_MAX from tbl GROUP BY assetExternalID) c1 on a1.assetExternalID = c1.assetExternalID
WHERE lower(a1.assetName) NOT LIKE lower('%do%not%use%') AND lower(a1.distributor) NOT LIKE lower('%nbc%test%') -- filter out null values as well
ORDER BY a1.Video_Series_Name, CAST(a1.SeasonNumber AS DECIMAL), CAST(a1.EpisodeNumber AS DECIMAL)
  , a1.assetName, CAST(a1.cuePointPosition AS DECIMAL), assetName
)

select *, 
current_date("America/New_York")-1 as updated_date
from tbl2
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35 -- final remove duplicates
