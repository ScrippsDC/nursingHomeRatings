CREATE MACRO ratings_by_provider_over_time(provnum) as TABLE

select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2020/08/NH_ProviderInfo_Aug2020.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2020/09/NH_ProviderInfo_Sep2020.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2020/10/NH_ProviderInfo.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2020/11/NH_ProviderInfo_Nov2020.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2020/12/NH_ProviderInfo_Nov2020.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/01/NH_ProviderInfo_Jan2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/02/NH_ProviderInfo_Feb2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/03/NH_ProviderInfo_Mar2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/04/NH_ProviderInfo_Apr2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/05/NH_ProviderInfo_May2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/06/NH_ProviderInfo_Jun2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/07/NH_ProviderInfo_Jul2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/08/NH_ProviderInfo_Aug2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/09/NH_ProviderInfo_Sep2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/10/NH_ProviderInfo_Oct2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/11/NH_ProviderInfo_Oct2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2021/12/NH_ProviderInfo_Nov2021.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/01/NH_ProviderInfo_Jan2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/02/NH_ProviderInfo_Feb2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/03/NH_ProviderInfo_Mar2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/04/NH_ProviderInfo_Apr2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/05/NH_ProviderInfo_May2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/06/NH_ProviderInfo_Jun2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/07/NH_ProviderInfo_Jul2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/08/NH_ProviderInfo_Aug2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/09/NH_ProviderInfo_Sep2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/10/NH_ProviderInfo_Oct2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/11/NH_ProviderInfo_Oct2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2022/12/NH_ProviderInfo_Nov2022.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/01/NH_ProviderInfo_Jan2023.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/02/NH_ProviderInfo_Feb2023.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/03/NH_ProviderInfo_Mar2023.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/04/NH_ProviderInfo_Apr2023.parquet' where "Federal Provider Number" like provnum
UNION ALL
select "Federal Provider Number", "Provider Name", "Provider State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/05/NH_ProviderInfo_May2023.parquet' where "Federal Provider Number" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/06/NH_ProviderInfo_Jun2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/07/NH_ProviderInfo_Jul2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/08/NH_ProviderInfo_Aug2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/09/NH_ProviderInfo_Sep2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/10/NH_ProviderInfo_Oct2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/11/NH_ProviderInfo_Oct2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2023/12/NH_ProviderInfo_Nov2023.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2024/01/NH_ProviderInfo_Jan2024.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2024/02/NH_ProviderInfo_Feb2024.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2024/03/NH_ProviderInfo_Mar2024.parquet' where "CMS Certification Number (CCN)" like provnum
UNION ALL
select  "CMS Certification Number (CCN)","Provider Name", "State", "Overall Rating", "Health Inspection Rating", "Processing Date" from '../data/source/nursinghome-compare/2024/04/NH_ProviderInfo_Apr2024.parquet' where "CMS Certification Number (CCN)" like provnum;