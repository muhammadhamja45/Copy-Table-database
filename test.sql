select 
trx_type,
response_code_merge,
kategori_rc,
COUNT(*) jumlah
from (
select *,
concat(new_respond,' (',resultstr_48,')') as response_code_merge,
row_number() over(partition by dt_atm_6, shortid_31, trx_type, resultstr_48) as nomor
from (
select -- * 
*,
CASE
	WHEN resultstr_48 in ('9910') 	   THEN 'Cancel Cardless Transaction'
    WHEN resultstr_48 in ('9908') 	   THEN 'Abort transaction on language selection'
    WHEN resultstr_48 in ('9943') 	   THEN 'CST Cancel Change Pin'
	WHEN resultstr_48 in ('9912') 	   THEN 'Abort on input amount'
	WHEN resultstr_48 in ('9911') 	   THEN 'CST Cardless Customer Timeout'
    WHEN resultstr_48 in ('9909') 	   THEN 'CST Cancel on fast cash menu'
    WHEN resultstr_48 in ('9931') 	   THEN 'CST Change PIN Timeout' 
    WHEN resultstr_48 in ('9905') 	   THEN 'PINPAD Error'
    WHEN resultstr_48 in ('1000') 	   THEN 'No Connection To Host'
	WHEN resultstr_48 in ('9001','9001') THEN 'EMoney History Print Failed'
    WHEN resultstr_48 in ('9000','9000') THEN 'EMoney History Print Success'	
    WHEN resultstr_48 in ('8000','8000') THEN 'EMoney Balance Inquiry Approved'
    WHEN resultstr_48 in ('9901','1901') THEN 'CST Cancel Input PIN'
    WHEN resultstr_48 in ('9902','1902') THEN 'CST Cancel Screenflow Features'
    WHEN resultstr_48 in ('9903','1903') THEN 'CST Cancel Confirmation'
	WHEN resultstr_48 in ('9913')        THEN 'Transaction cardless withdrawal time out'
    WHEN resultstr_48 in ('9904','1904') THEN 'CST Cancel Continued Transaction'
    WHEN resultstr_48 in ('9951','1951') THEN 'CST Timeout Input PIN'
    WHEN resultstr_48 in ('9952','1952') THEN 'CST Timeout Screenflow Features'
    WHEN resultstr_48 in ('9953','1953') THEN 'CST Timeout Confirmation'
    WHEN resultstr_48 in ('9954','1954') THEN 'CST Timeout Ask Continued Transaction'
    WHEN resultstr_48 in ('9966','2966') THEN 'Card Has Been Retained'
    WHEN resultstr_48 in ('9975','2975') THEN 'Transaction Cancel Amount Cant Process'
    WHEN resultstr_48 in ('9977','2977') THEN 'Transaction Blocked, Feature is not yet available'
    WHEN resultstr_48 in ('9978','2978') THEN 'Transaction Blocked Flipped Card'
    WHEN resultstr_48 in ('9993','2993') THEN 'Chip Error, Fallback not allowed'
    WHEN resultstr_48 in ('9995','2995') THEN 'EMV Process Error, Transaction Failed'
    WHEN resultstr_48 in ('9996','2996') THEN 'PINpad Error when input PIN'
    WHEN resultstr_48 in ('9950','2960') THEN 'Set Bin Group Error, EMV Failed'
	WHEN resultstr_48 in ('0F21') 	   THEN 'Failed to Dispense'
	WHEN resultstr_48 in ('1021') 	   THEN 'Track 2 blank (chip only / magnetic stripe)'
	WHEN resultstr_48 in ('88') 	   	   THEN 'System error from bank)'
	WHEN resultstr_48 in ('89') 	       THEN 'Timed out From Bank)'
	WHEN resultstr_48 in ('0C20') 	   THEN 'Schedule Admin Not Found'
	WHEN resultstr_48 in ('AL') 	  	   THEN 'Multi Account Indicator'
	WHEN resultstr_48 in ('B5') 	       THEN 'Bill not Found / card not registered in XLS system'
	WHEN resultstr_48 in ('B8') 	       THEN 'Bill Already Paid'
	WHEN resultstr_48 in ('C3') 	       THEN 'Limit Exceeeded'
	WHEN resultstr_48 in ('F1') 	       THEN 'Limit Reject from Bank, Fallback not allowed'
	WHEN resultstr_48 in ('M2') 	       THEN 'Multi Response'
	WHEN resultstr_48 in ('P7') 	       THEN 'Emoney zero amount or no balance update required'
	WHEN resultstr_48 in ('S4') 	       THEN 'Emoney Digital Signature Fail'
	WHEN resultstr_48 in ('V6') 	       THEN 'SKIM Force PINM'
  WHEN `resultstr_48` in ('Q8') 	       THEN 'Account Limit Exceeded (Emoney Mandiri)'
	WHEN `resultstr_48` in ('Q4') 	       THEN 'Host Timeout Trx Topup Pending BRIZZI'
	WHEN `resultstr_48` in ('1997') 	       THEN 'Transaction blocked, Pinblock Null'
	WHEN `resultstr_48` in ('C0') 	     THEN 'Bill has been suspended, please contact provider'
	WHEN `resultstr_48` in ('P3') 	     THEN 'Emoney card status problem'
	WHEN `resultstr_48` in ('P6') 	     THEN 'Emoney general error or terminalID not register'
	WHEN `resultstr_48` in ('Z0') 	     THEN 'Invalid Nominal'
	WHEN `resultstr_48` in ('Z1') 	     THEN 'TID Not Linked to Card'
    ELSE response_code
  END AS new_respond
  from (
select
 x0_.mandant_id,
 x0_.cardstatus,
 x0_.state,
 x0_.atm_id,
 x0_.bin_card,
 x0_.tid_3, 
 x0_.dt_server_5, 
 x0_.dt_atm_6, 
 x0_.amount_7,
 x0_.total_trx, 
 x0_.currency_8, 
 x0_.sclr_14, 
 x0_.servicecode_17, 
 x0_.beleg_18,  
 x0_.proccode_28,  
 x0_.location_37, 
 x0_.posentry_38, 
 x0_.resultstr_48, 
 x0_.sclr_49, 
 x0_.cancel_reason,
 upper(t1_.name) AS name_20, 
 t1_.technical AS technical_21,
 a2_.descriptiontext AS descriptiontext_30, 
 a2_.shortid AS shortid_31, 
 l3_.comment AS comment_35, 
 l3_.description AS description_36, 
 SUBSTRING_INDEX(m4_.name, '_', -1)  AS name_46, 
 a5_.name AS name_47, 
 l6_.description AS description_50, 
 c7_.name AS name_52, 
 x0_.result,
 case 
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9910','9904','9908','9943','9902','9906','9912') then 'Pinpad Cancel'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9911','9909','9931','9905','9907','1000') then 'Timeout/No Activity'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9999') then 'Undefined'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9950') then 'Get BIN Group Failed'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9930') then 'System Error'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 in ('9932') then 'Pinpad Error'
 else a5_.name end as response_code,
 case 
   when x0_.resultstr_48 in ('00','8000','9000') then 'Success'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 like '00%' then 'Anomaly'
   when x0_.resultstr_48 in ('0068', '0091', '0200', '0097') then 'Anomaly'
   when x0_.resultstr_48 in ('9930','9932','9950','9993','9995','9977') and x0_.resultstr_48 != '76' then 'Gagal Sistem'
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 like '99%' then 'Gagal Nasabah' 
   when length(x0_.resultstr_48) = 4 and x0_.resultstr_48 like '19%' then 'Gagal Nasabah' 
   when x0_.resultstr_48 in ('13','17','18','19','44','51','52','53','55','61','65','75','76') then 'Gagal Nasabah'
 else 'Gagal Sistem' end kategori_rc,

 case 
   when t1_.name in ('BALANCE INQUIRY') then 'BI'
   when t1_.name in ('BILL PAYMENT') then 'BP'
   when t1_.name in ('BILL PAYMENT INQ') then 'BP INQ'
   when t1_.name in ('CASH WITHDRAWAL') then 'CW'
   when t1_.name in ('FAST CASH') then 'FC'
   when t1_.name in ('MINI STATEMENT') then 'MS'
   when t1_.name in ('TRANSFER TO OTHER ACCOUNT') then 'TR'
   when t1_.name in ('TRANSFER TO OTHER INQUIRY') then 'TR INQ'
   when t1_.name in ('CASH IN') then 'CI'
   when t1_.name in ('CASH OUT') then 'CO'
   when t1_.name in ('MOBILE CASHOUT') then 'MB CO'
   when t1_.name in ('PENDING TOPUP BALANCE') then 'PND TB'
   when t1_.name in ('PENDING TOPUP INQ') then 'PND TP INQ'
   when t1_.name in ('PENDING TOPUP PAYMENT') then 'PND TP PY'
   when t1_.name in ('PIN CHANGE') then 'PC'
   when t1_.name in ('POIN BRI') then 'PB'
   when t1_.name in ('REGISTRATION') then 'RG'
   when t1_.name in ('REGISTRATION INQ') then 'RG INQ'
   when t1_.name in ('TIA') then 'TIA'
   when x0_.resultstr_48 in ('0F21') then 'CW'
   when x0_.resultstr_48 in ('8000') then 'E-Money'
   when x0_.resultstr_48 in ('9000') then 'E-Money'
   when x0_.resultstr_48 in ('9912') then 'CW'
   when t1_.name = '' then ''
 else 'Other' end trx_type
 from
 (
  SELECT 
    t0_.atm_id,
    t0_.mandant_id,
    t0_.cardstatus,
    left(t0_.vispan ,6) as bin_card,
    t0_.tid AS tid_3, 
    DATE_FORMAT(t0_.dt_server, "%Y-%m-%d %H:00:00") AS dt_server_5,
    DATE_FORMAT(t0_.dt_atm, "%Y-%m-%d %H:00:00") AS dt_atm_6,
    sum(t0_.amount) AS amount_7,
    count(distinct trace) as total_trx, 
    t0_.currency AS currency_8, 
    t0_.dcc_currency AS sclr_14, 
    t0_.servicecode AS servicecode_17, 
    t0_.beleg AS beleg_18,  
    t0_.proccode AS proccode_28,  
    t0_.state AS state, 
    t0_.location AS location_37, 
    t0_.posentry AS posentry_38, 
    t0_.resultstr AS resultstr_48, 
    t0_.cardstatus AS sclr_49, 
    t0_.cancel_reason,
    t0_.type,
    t0_.result
  FROM transaction t0_ 
    -- WHERE (t0_.dt_server BETWEEN '2024-03-20 00:00:00' AND '2024-03-20 23:59:59')
  WHERE (t0_.dt_server BETWEEN $__timeFrom() AND $__timeTo())
  GROUP BY
    t0_.atm_id,
    t0_.mandant_id,
    t0_.cardstatus,
    left(t0_.vispan ,6),
    t0_.tid, 
    t0_.dt_server,
    t0_.dt_atm,
    t0_.amount,
    t0_.currency, 
    t0_.dcc_currency, 
    t0_.servicecode, 
    t0_.beleg,  
    t0_.proccode,  
    t0_.state, 
    t0_.location, 
    t0_.posentry, 
    t0_.resultstr,  
    t0_.cancel_reason,
    t0_.type,
    t0_.result
  )x0_
 LEFT JOIN atm a2_ ON x0_.atm_id = a2_.id 
 LEFT JOIN cancelreasons c7_ ON x0_.cancel_reason = c7_.reason 
 LEFT JOIN transaction_type t1_ ON x0_.type = t1_.id
 LEFT JOIN mandant m4_ ON x0_.mandant_id = m4_.id 
 LEFT JOIN lookup l6_ ON x0_.cardstatus = l6_.key AND l6_.lookup_type_id IN ('30') 
 LEFT JOIN lookup l3_ ON x0_.state = l3_.key AND l3_.lookup_type_id IN ('6') 
 LEFT JOIN atm_result a5_ ON x0_.result = a5_.code 
  WHERE SUBSTRING_INDEX(m4_.name, '_', -1) in ('BNI') 
 -- AND tid_3 in ($terminal_id)
-- limit 200
) aa
) bb
-- where response_code is NULL 
) cc
where 
    (kategori_rc = 'Anomaly' and nomor = 1)
    or kategori_rc != 'Anomaly'
group  by response_code_merge,trx_type,kategori_rc
order by jumlah desc