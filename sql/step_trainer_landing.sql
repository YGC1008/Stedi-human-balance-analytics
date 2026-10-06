CREATE EXTERNAL TABLE IF NOT EXISTS `yared`.`step_trainer_landing` (
    `sensorreadingtime` bigint,
    `serialnumber` string,
    `distancefromobject` double
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://yared-gomez/step_trainer/landing/';