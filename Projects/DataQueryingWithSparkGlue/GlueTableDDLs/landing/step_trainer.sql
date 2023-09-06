CREATE EXTERNAL TABLE `landing_step_trainer`(
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  `distancefromobject` int COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://glue-kstine-bucket-udacity/Project_GlueSpark/landing/step_trainer/'
TBLPROPERTIES (
  'TableType'='EXTERNAL_TABLE',
  'classification'='json')