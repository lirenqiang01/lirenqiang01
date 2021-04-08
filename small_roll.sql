
select 
       SUBSTR(aa.order_pay_time,1,10) `推广时间`
      ,count(DISTINCT aa.out_order_id) as `低价课订单量`
-- 			concat(round(count(distinct case when unionid<>'' then no end)/count(distinct no)*100,2),'%') as`微信进量占比`

      ,count(DISTINCT bb.order_no) as `小程序UV`
			,concat(round(count(DISTINCT bb.order_no) / count(DISTINCT aa.out_order_id) *100,2),'%') `小程序访问率`
			
			,count(DISTINCT cc.order_no) as `小程序授权人数`
			,concat(round(count(DISTINCT cc.order_no) / count(DISTINCT bb.order_no) * 100,2),'%') `小程序授权率`
			
       ,count(DISTINCT dd.union_id) as `公众号二维码长按人数`
			,concat(round(count(DISTINCT dd.union_id) / count(DISTINCT cc.order_no)*100,2),'%') `公众号二维码长按率`

			,count(distinct ee.union_id) as `公众号关注人数`
			,concat(round(count(distinct ee.union_id) / count(DISTINCT dd.union_id)*100,2),'%') `公众号关注率`
			
			,count(DISTINCT ff.external_unionid) `添加销售好友人数`
			,concat(round(count(DISTINCT ff.external_unionid) / count(distinct ee.union_id) *100,2),'%') `关注后加销售率`
			,concat(round(count(DISTINCT ff.external_unionid) / count(DISTINCT cc.order_no)* 100,2),'%') `有效用户加销售好友率`
from 
 (select out_order_id,mobile,order_pay_time,channel_code
from `kkb-cloud-vipcourse`.vip_order 
where passback_params like '%SHAKING_SHOP%' 
and order_pay_time>='2021-03-21 00:00:00' 
-- and order_pay_time<'2021-03-28 00:00:00'
and channel_code in ( 'vyn5knp6kt','87djvygtz4','gtou9sso0v','n33qzczk5v','vbgf8b6ui1','phowr3rgnc',
											'6l5ypyd9c1','yscrriipl8','90g7ffcmrs','d4is8004qo','egksbjt5cf','sdhk5b5dsy',
											'yrveefrrpn','5tk338re33','ycdfu3a62i','5ifb62wvrl','j014vplixl','9nt5mbqfc3',
											'7b1h4wsgc2','mw9sxxejv1','pg2dzvt2wo','i87upmmoac','5iqi9ze6v6','2yo2n466ms',
											'gktw3rf2b9','0ma7tqw4ky','ix33gv87ma','4splvmurnn','av9ks57ze5','62gndtcwv2',
											'bez06dnuex','8yvf9xhqih','ncl07wq7ap','tlqb9ge76b','sx62wet0li','khvwl9105k',
											'hpqq8ekzd9','1c7ekot9qw','cqs2azltjj','f9tt7bgxz8','2u1tl2vzfw','xf3h67lmjh',
											'p7vay30ol8','qb0iqc6178','fid7w0eiyg','2tpgmndt6t','4ut2w4k8bm','v6x5acvxus',
											'exu0ht9ygu','enytfu55ot','1y6betz7e5','jkt17vwyql','vefzai7w48','bq2a26j5d9',
											'10ugit6zv0','w2fqxxt1al','j3irrc8e1a','fgo1ks3ve8','rfgepf22e5','nuwv6go4po',
											'nnqz6o637s','k78fvsc86k','yz9sysdzbo','0ucddzm7sr','cevsjdqdyt','c29zxz8di4',
											'8pkf1nieox','xlmrp73a7a','k7u9r6c1yb'
)
) aa

LEFT JOIN

-- 小程序UV 2

(SELECT distinct mobile
		 ,order_no
-- 		 ,visit_time -- 访问小程序时间(时间挫)
from `kkb-cloud-vipcourse`.app_visit_record
where from_unixtime(create_at)>='2021-03-21 00:00:00' 
and order_no in (select `no` 
                       from `kkb-cloud-vipcourse`.vip_order 
											 where passback_params like '%SHAKING_SHOP%' 
											 and order_pay_time>='2021-03-21 00:00:00' 
-- 											 and order_pay_time<'2021-03-28 00:00:00'
											 )) bb on aa.mobile = bb.mobile
										 
LEFT JOIN
-- 
-- --小程序授权人数 3
-- SELECT count(aa.unionid)
-- from (
(SELECT unionid,mobile,order_no
from `kkb-cloud-vipcourse`.vip_order_unionid
where create_time>='2021-03-21 00:00:00' 
and order_no in (select `no` 
                  from `kkb-cloud-vipcourse`.vip_order 
									where passback_params like '%SHAKING_SHOP%' 
									and order_pay_time>='2021-03-21 00:00:00' 
-- 									and order_pay_time<'2021-03-28 00:00:00'
									)) cc on cc.mobile = bb.mobile

LEFT JOIN

-- EXPLAIN
-- 公众号二维码长按人数 4
		(select distinct wsr.union_id
		from 
		(select union_id
		from `kkb-cloud-vipcourse`.wechat_scan_record
		where FROM_UNIXTIME(create_at)>= '2021-03-21 00:00:00'
		and EVENT in ('subscribe','scan')) wsr 
		join
		(SELECT unionid,order_no
		from `kkb-cloud-vipcourse`.vip_order_unionid
		where create_time>='2021-03-21 00:00:00') vou on wsr.union_id = vou.unionid
		join
		(select `no` from `kkb-cloud-vipcourse`.vip_order 
		where passback_params like '%SHAKING_SHOP%' 
		and order_pay_time>='2021-03-21 00:00:00' 
-- 		and order_pay_time<'2021-03-28 00:00:00'
		) vo on vo.no = vou.order_no) dd on dd.union_id = cc.unionid
LEFT JOIN
-- --公众号关注人数  5
		(select distinct wsr.union_id
		from 
		(select union_id
		from `kkb-cloud-vipcourse`.wechat_scan_record
		where FROM_UNIXTIME(create_at)>= '2021-03-21 00:00:00'
		and EVENT = 'subscribe') wsr 
		join
		(SELECT unionid,order_no
		from `kkb-cloud-vipcourse`.vip_order_unionid
		where create_time>='2021-03-21 00:00:00') vou on wsr.union_id = vou.unionid
		join
		(select `no` from `kkb-cloud-vipcourse`.vip_order 
		where passback_params like '%SHAKING_SHOP%' 
		and order_pay_time>='2021-03-21 00:00:00' 
-- 		and order_pay_time<'2021-03-28 00:00:00'
		) vo on vo.no = vou.order_no) ee on ee.union_id = dd.union_id
LEFT JOIN
(
-- 添加销售人数
select ecad.external_unionid
from `kkb-cloud-enterprise-wechat`.enterprise_customer_add_record ecad
where ecad.create_time>='2021-03-21 00:00:00'
-- and `status` = 1 -- 有效添加销售人数

) ff on ff.external_unionid = ee.union_id
GROUP BY SUBSTR(aa.order_pay_time,1,10) with ROLLUP
