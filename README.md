# 任务四：自定义任务  

## 设计思路  
  分析所给时间范围内， {(每个月)(每个星座的人群)的人均月消费}，并在每个月内，对每个星座的数据进行降序排列.  
  需要用的数据为，user_balance_table中的user_id, report_date, consume_amt   
  以及user_profile_table中的user_id, constellation  

  分为两个mapreduce任务  
  第一个任务有两种mapper，分别处理user_balance_table和user_profile_table的数据，  
                        一类mapper输出key：userId；  value："constellation:星座"  
                        一类mapper输出key：userId；  value："spending:月份,单日消费额"  
  第一个任务的reducer得到输入：key:userId;    value:"constellation:xxx","spending:xxx","spending:xxx","spending:xxx","spending:xxx"  
                        由于数据缺失，有些user没有星座数据好像，所以，value也可能是"spending:xxx","spending:xxx","spending:xxx","spending:xxx"  
  这个reducer累加一个人一个月的消费额，累加属于每个星座的人数，累加（有星座数据的）（每个星座）所有user的（每月）总月消费额，并除以相应星座的人数，得到每个星座每个月人均月消费额  

  第二个mapreduce任务中，将月份和人均月消费额作为二元的key，利用框架进行自动排序，并定义了partitioner 
  
## 实验结论  
  从不同星座排名的交替轮换可知，用户星座与其消费行为关联性低。另外，可看出在余额宝推出初期，用户消费逐月增长。  

  
## 运行结果  

201405:	水瓶座	65193.13827054795
201405:	巨蟹座	60080.93006357856
201405:	摩羯座	58016.17719298245
201405:	天秤座	57099.60721649484
201405:	双鱼座	53881.250331125826
201405:	白羊座	51361.562248995986
201405:	处女座	49551.62354825791
201405:	射手座	49411.4323630137
201405:	金牛座	47387.67125237192
201405:	狮子座	45665.71931294512
201405:	天蝎座	45043.476136363635
201405:	双子座	42029.19588122606
201406:	巨蟹座	81230.95231607629
201406:	天秤座	66713.80343642611
201406:	射手座	65115.85145547945
201406:	双鱼座	59289.03973509934
201406:	双子座	59230.877394636016
201406:	白羊座	57135.82128514056
201406:	水瓶座	56329.583476027394
201406:	狮子座	55647.366149979054
201406:	处女座	53417.81938325991
201406:	天蝎座	49676.67575757576
201406:	摩羯座	49579.62807017544
201406:	金牛座	46494.24098671727
201407:	巨蟹座	75565.39373297003
201407:	摩羯座	74702.49780701754
201407:	白羊座	68140.53313253012
201407:	天秤座	62966.70034364261
201407:	双子座	62925.0311302682
201407:	射手座	62569.630993150684
201407:	双鱼座	62565.20264900662
201407:	天蝎座	61902.872348484845
201407:	水瓶座	59983.165239726026
201407:	狮子座	58254.048596564724
201407:	金牛座	55116.51612903226
201407:	处女座	51770.442130556665
201408:	射手座	89828.41352739726
201408:	双鱼座	86417.15452538632
201408:	摩羯座	82649.8697368421
201408:	双子座	78767.02011494253
201408:	金牛座	78024.45825426946
201408:	天蝎座	76285.81780303031
201408:	白羊座	73316.12449799197
201408:	狮子座	72159.6568914956
201408:	水瓶座	71803.75727739726
201408:	天秤座	67240.43333333333
201408:	巨蟹座	60378.52770208901
201408:	处女座	55990.85702843412


## web页面截图  
<img width="415" alt="image" src="https://github.com/user-attachments/assets/3ec05a89-6eb8-4f6f-b0bc-0db2b1572ed1">  

## 可能的改进之处  
第一个reducer的任务可能比较重了，可以想办法简化。  
另外，可以进一步统计每个星座排名记录，比如几次第1，几次第2...，如果是这样的话，统计时间片段可以考虑进一步缩小至每天  
