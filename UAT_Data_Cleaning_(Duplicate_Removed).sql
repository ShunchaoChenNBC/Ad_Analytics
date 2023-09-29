CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.ad_exp_cue_point_summary_no_duplicates` as



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
    ON LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(a.assetExternalID,'_UHDDV',''),'_HDSDR',''),'_UHDSDR',''),'_UHDHDR','')) = LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(b.ContentID,'_UHDDV',''),'_HDSDR',''),'_UHDSDR',''),'_UHDHDR',''))
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
WHERE lower(a1.assetName) NOT LIKE lower('%do%not%use%') 
  -- AND lower(a1.distributor) NOT LIKE lower('%nbc%test%') -- filter out null values as well so it filter out titles like "the office" S2E18
ORDER BY a1.Video_Series_Name, CAST(a1.SeasonNumber AS DECIMAL), CAST(a1.EpisodeNumber AS DECIMAL)
  , a1.assetName, CAST(a1.cuePointPosition AS DECIMAL), assetName
),

remove_duplicates as (
select *
from tbl2
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -- final remove duplicates
),


----set up a reference table to detect the null values in Primary Genre, Secondary Genre, Product Type for some titles like "office S2E18"
ref_table as (
select Video_Series_Name,min(Primary_Genre) as Primary_Genre,min(Secondary_Genre) as Secondary_Genre,min(ProductType) as ProductType -- avoid 1 to many situation
from remove_duplicates
where Video_Series_Name in (
select Video_Series_Name
from remove_duplicates
where Primary_Genre is null and Secondary_Genre is null and ProductType is null and SeasonNumber != 0 and EpisodeNumber != 0
group by 1)
and Primary_Genre is not null and Secondary_Genre is not null and ProductType is not null
group by 1
),

Filled_CTE as (
select 
d.Video_Series_Name,
assetExternalID,
assetName,
assetDuration,
Asset_Duration_minutes,
createdAt,
agingDate,
cuePointLength,
cuePointPosition,
contentTimePosition,
Content_Breaks,
Content_Breaks_percent,
ad_cue,
Content_Segments,
Content_Segments_percent,
duration, -- deal with duration null value on dashboard
case when d.Primary_Genre is null and SeasonNumber != 0 and EpisodeNumber != 0 then r.Primary_Genre else d.Primary_Genre end as Primary_Genre, -- fill null value in some titles
case when d.Secondary_Genre is null and SeasonNumber != 0 and EpisodeNumber != 0 then r.Secondary_Genre else d.Secondary_Genre end as Secondary_Genre,-- fill null value in some titles
case when d.ProductType is null and SeasonNumber != 0 and EpisodeNumber != 0 then r.ProductType else d.ProductType end as ProductType, -- fill null value in some titles
SeasonNumber,
EpisodeNumber,
TypeOfContent,
Distributor,
CoppaCompliance,
adRequirementsOnAVOD,
adRequirementsOnPremiumTier,
adRequirementsOnPremiumPlusTier,
Rev_Share,
ad_spec,
ad_grade,
Mutiplier,
Multiplier_just_one_more,
Content_Segments_MAX,
Content_Segments_MAX_split,
from remove_duplicates d
left join ref_table r on r.Video_Series_Name = d.Video_Series_Name
),


Combination as (
(select *,
cuePointPosition as Interval_Segments -- Add to calculate the interval between last cue point and the end of the video
from Filled_CTE)
union all
(select 
Video_Series_Name,
assetExternalID,
assetName,
assetDuration,
null as Asset_Duration_minutes,
null as createdAt,
null as agingDate,
cuePointLength,
cuePointLength+1 as cuePointPosition,
assetDuration as contentTimePosition,
null as Content_Breaks,
null as Content_Breaks_percent,
null as ad_cue,
null as Content_Segments,
null as Content_Segments_percent,
duration, -- deal with duration null value on dashboard
Primary_Genre,
Secondary_Genre,
ProductType,
SeasonNumber,
EpisodeNumber,
TypeOfContent,
null as Distributor,
null as CoppaCompliance,
null as adRequirementsOnAVOD,
null as adRequirementsOnPremiumTier,
null as adRequirementsOnPremiumPlusTier,
null as Rev_Share,
ad_spec,
ad_grade,
null as Mutiplier,
null as Multiplier_just_one_more,
null as Content_Segments_MAX,
null as Content_Segments_MAX_split,
cuePointLength+1 as Interval_Segments
from Filled_CTE
group by Video_Series_Name, assetExternalID, assetName, assetDuration, cuePointLength, cuePointPosition, contentTimePosition,duration,Primary_Genre,
Secondary_Genre,ProductType, Interval_Segments, SeasonNumber, EpisodeNumber, TypeOfContent, ad_spec, ad_grade)
), --- add this section to calculate the intervals between last cue point to the end

Combination_Ad_Spec as (
select
Video_Series_Name,
assetExternalID,
assetName,
assetDuration,
Asset_Duration_minutes,
createdAt,
agingDate,
cuePointLength,
cuePointPosition,
contentTimePosition,
Content_Breaks,
Content_Breaks_percent,
ad_cue,
Content_Segments,
Content_Segments_percent,
duration, -- deal with duration null value on dashboard
Primary_Genre,
Secondary_Genre,
ProductType,
SeasonNumber,
EpisodeNumber,
TypeOfContent,
Distributor,
CoppaCompliance,
adRequirementsOnAVOD,
adRequirementsOnPremiumTier,
adRequirementsOnPremiumPlusTier,
Rev_Share,
CASE WHEN assetDuration IS NULL THEN 0 -- NP
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 and Primary_Genre = "Movies" THEN 0
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 and Primary_Genre = "Movies" THEN 2
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 and Primary_Genre = "Movies" THEN 3
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 and Primary_Genre = "Movies" THEN 5
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 and Primary_Genre = "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 and Primary_Genre = "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 and Primary_Genre = "Movies" THEN 10
        WHEN SAFE_CAST(assetDuration AS INT64)/60 < 180 and Primary_Genre = "Movies" THEN 13
        WHEN SAFE_CAST(assetDuration AS INT64)/60 >= 180 and Primary_Genre = "Movies" THEN 15 -- add additional bracket to separate Movie and other (TV) bracklets
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 10 and Primary_Genre != "Movies" THEN 0
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 20 and Primary_Genre != "Movies" THEN 2
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 40 and Primary_Genre != "Movies" THEN 3
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 60 and Primary_Genre != "Movies" THEN 5
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 90 and Primary_Genre != "Movies" THEN 6
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 120 and Primary_Genre != "Movies" THEN 8
         WHEN SAFE_CAST(assetDuration AS INT64)/60 < 150 and Primary_Genre != "Movies" THEN 10
         ELSE 13
         END AS ad_spec, -- re-calculate ad_spec to avoid 13 for null value
ad_grade,
Mutiplier,
Multiplier_just_one_more,
Content_Segments_MAX,
Content_Segments_MAX_split,
Interval_Segments
from Combination
),


Combination_Ad_Grade as (
select
Video_Series_Name,
assetExternalID,
assetName,
assetDuration,
Asset_Duration_minutes,
createdAt,
agingDate,
cuePointLength,
cuePointPosition,
contentTimePosition,
Content_Breaks,
Content_Breaks_percent,
ad_cue,
Content_Segments,
Content_Segments_percent,
duration, -- deal with duration null value on dashboard
Primary_Genre,
Secondary_Genre,
ProductType,
SeasonNumber,
EpisodeNumber,
TypeOfContent,
Distributor,
CoppaCompliance,
adRequirementsOnAVOD,
adRequirementsOnPremiumTier,
adRequirementsOnPremiumPlusTier,
Rev_Share,
ad_spec, 
CASE WHEN cuePointLength IS NULL AND assetDuration IS NULL   THEN "NULL"
      WHEN cuePointLength IS NULL AND ad_spec = 0               THEN "At Spec"
      WHEN cuePointLength IS NULL                               THEN "Below Spec"
      WHEN SAFE_CAST(cuePointLength AS INT64) > ad_spec          THEN "Above Spec"
      WHEN SAFE_CAST(cuePointLength AS INT64) = ad_spec          THEN "At Spec"
      WHEN SAFE_CAST(cuePointLength AS INT64) < ad_spec          THEN "Below Spec"
      END AS ad_grade,-- re-calculate ad_grade to avoid inccuracy of ad spec
Mutiplier,
Multiplier_just_one_more,
Content_Segments_MAX,
Content_Segments_MAX_split,
Interval_Segments
from Combination_Ad_Spec
),

-- avoid duplicated in aging date and filter out all irrelvant columns to reduce duplication risk
Final_remove_duplicates as (
select 
Video_Series_Name,
assetExternalID,
assetName,
assetDuration,
Asset_Duration_minutes,
cuePointLength,
cuePointPosition,
contentTimePosition,
Content_Breaks,
Content_Breaks_percent,
ad_cue,
Content_Segments,
Content_Segments_percent,
duration,
Primary_Genre,
Secondary_Genre,
ProductType,
SeasonNumber,
EpisodeNumber,
TypeOfContent,
Distributor,
ad_spec,
ad_grade,
Mutiplier,
Multiplier_just_one_more,
Content_Segments_MAX,
Content_Segments_MAX_split,
Interval_Segments
from Combination_Ad_Grade
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28 -- final remove duplicates
),


tbl3 as (select *, 
lag(contentTimePosition) over (partition by Video_Series_Name,SeasonNumber,EpisodeNumber order by Interval_Segments) as Last_contentTimePosition,
current_date("America/New_York")-1 as updated_date
from Final_remove_duplicates)

select *,
round((contentTimePosition - ifnull(Last_contentTimePosition,0)),2) as Ad_Breaks_Interval
from tbl3
