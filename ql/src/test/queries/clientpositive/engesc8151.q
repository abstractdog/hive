create table jar_sf_COAlertMst
(
    ID                          String,
    OWNERID                     String,
    ISDELETED                   String,
    NAME                        String,
    RECORDTYPEID                String,
    CREATEDDATE                 String,
    LASTMODIFIEDDATE            String,
    ALARTCD__C                  String,
    DSGPERSON__C                String,
    GENSYOU_P_U_ZG_HOUKOKUHI__C String,
    GENSYOU_P_U_ZK_HOUKOKUHI__C String,
    GENSYOU_P_ZZG__C            String,
    HEIKIN12_P__C               String,
    ZG_HI_K__C                  String,
    ZG_HI_P_U__C                String,
    ZG_K_GENSYOU__C             String,
    ZG_K_ZOUKA__C               String,
    ZG_P_GENSYOU__C             String,
    ZG_P_ZOUKA__C               String,
    ZG_P__C                     String,
    ZK_K_GENSYOU__C             String,
    ZK_K_ZOUKA__C               String,
    ZOUKA_P_U_ZG_HOUKOKUHI__C   String,
    ZOUKA_P_U_ZK_HOUKOKUHI__C   String,
    ZZG_K_GENSYOU__C            String,
    SEIZYOUMIKAN_K__C           String,
    KEISANTI_UP_P__C            String,
    GAITOU_UP__C                String,
    GAITIOU_UNDER_K__C          String,
    KEISANTI_UNDER_P__C         String,
    ERROR_UP_K__C               String,
    K_UP__C                     String,
    KEIYAKUSYOUKAINASHI_UP_K__C String,
    GAITOUALERT_K__C            String,
    KEIYAKU_K_DOWN__C           String,
    MIKANALERT_K_UP__C          String,
    GAINASHIRUINASHI_K_DOWN__C  String,
    STPC3_K_UP__C               String,
    KEISANTI_P_MIMAN__C         String,
    KEISANTI_P__C               String
)
    row format delimited fields terminated by "\t"
;

LOAD DATA LOCAL INPATH '../../data/files/engesc8151.txt' OVERWRITE INTO TABLE jar_sf_COAlertMst;

create table jar_sf_COAlertMst2 row format delimited fields terminated by "\t" as
select ID
     , OWNERID
     , ISDELETED
     , NAME
     , RECORDTYPEID
     , CREATEDDATE
     , LASTMODIFIEDDATE
     , ALARTCD__C
     , DSGPERSON__C
     , GENSYOU_P_U_ZG_HOUKOKUHI__C
     , GENSYOU_P_U_ZK_HOUKOKUHI__C
     , GENSYOU_P_ZZG__C
     , HEIKIN12_P__C
     , ZG_HI_K__C
     , ZG_HI_P_U__C
     , ZG_K_GENSYOU__C
     , ZG_K_ZOUKA__C
     , ZG_P_GENSYOU__C
     , ZG_P_ZOUKA__C
     , ZG_P__C
     , ZK_K_GENSYOU__C
     , ZK_K_ZOUKA__C
     , ZOUKA_P_U_ZG_HOUKOKUHI__C
     , ZOUKA_P_U_ZK_HOUKOKUHI__C
     , ZZG_K_GENSYOU__C
     , SEIZYOUMIKAN_K__C
     , KEISANTI_UP_P__C
     , GAITOU_UP__C
     , GAITIOU_UNDER_K__C
     , KEISANTI_UNDER_P__C
     , ERROR_UP_K__C
     , K_UP__C
     , KEIYAKUSYOUKAINASHI_UP_K__C
     , GAITOUALERT_K__C
     , KEIYAKU_K_DOWN__C
     , MIKANALERT_K_UP__C
     , GAINASHIRUINASHI_K_DOWN__C
     , STPC3_K_UP__C
     , KEISANTI_P_MIMAN__C
     , KEISANTI_P__C
from jar_sf_coalertmst
;

set hive.auto.convert.join=true;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
SELECT *
FROM jar_sf_COAlertMst T1
WHERE NOT EXISTS (SELECT *
                  FROM jar_sf_COAlertMst2 T2
                  WHERE NVL(T1.id, '') = NVL(T2.id, '')
                    AND NVL(T1.ownerid, '') = NVL(T2.ownerid, '')
                    AND NVL(T1.isdeleted, '') = NVL(T2.isdeleted, '')
                    AND NVL(T1.name, '') = NVL(T2.name, '')
                    AND NVL(T1.recordtypeid, '') = NVL(T2.recordtypeid, '')
                    AND NVL(T1.createddate, '') = NVL(T2.createddate, '')
                    AND NVL(T1.lastmodifieddate, '') = NVL(T2.lastmodifieddate, '')
                    AND NVL(T1.alartcd__c, '') = NVL(T2.alartcd__c, '')
                    AND NVL(T1.dsgperson__c, '') = NVL(T2.dsgperson__c, '')
                    AND NVL(T1.gensyou_p_u_zg_houkokuhi__c, '') = NVL(T2.gensyou_p_u_zg_houkokuhi__c, '')
                    AND NVL(T1.gensyou_p_u_zk_houkokuhi__c, '') = NVL(T2.gensyou_p_u_zk_houkokuhi__c, '')
                    AND NVL(T1.gensyou_p_zzg__c, '') = NVL(T2.gensyou_p_zzg__c, '')
                    AND NVL(T1.heikin12_p__c, '') = NVL(T2.heikin12_p__c, '')
                    AND NVL(T1.zg_hi_k__c, '') = NVL(T2.zg_hi_k__c, '')
                    AND NVL(T1.zg_hi_p_u__c, '') = NVL(T2.zg_hi_p_u__c, '')
                    AND NVL(T1.zg_k_gensyou__c, '') = NVL(T2.zg_k_gensyou__c, '')
                    AND NVL(T1.zg_k_zouka__c, '') = NVL(T2.zg_k_zouka__c, '')
                    AND NVL(T1.zg_p_gensyou__c, '') = NVL(T2.zg_p_gensyou__c, '')
                    AND NVL(T1.zg_p_zouka__c, '') = NVL(T2.zg_p_zouka__c, '')
                    AND NVL(T1.zg_p__c, '') = NVL(T2.zg_p__c, '')
                    AND NVL(T1.zk_k_gensyou__c, '') = NVL(T2.zk_k_gensyou__c, '')
                    AND NVL(T1.zk_k_zouka__c, '') = NVL(T2.zk_k_zouka__c, '')
                    AND NVL(T1.zouka_p_u_zg_houkokuhi__c, '') = NVL(T2.zouka_p_u_zg_houkokuhi__c, '')
                    AND NVL(T1.zouka_p_u_zk_houkokuhi__c, '') = NVL(T2.zouka_p_u_zk_houkokuhi__c, '')
                    AND NVL(T1.zzg_k_gensyou__c, '') = NVL(T2.zzg_k_gensyou__c, '')
                    AND NVL(T1.seizyoumikan_k__c, '') = NVL(T2.seizyoumikan_k__c, '')
                    AND NVL(T1.keisanti_up_p__c, '') = NVL(T2.keisanti_up_p__c, '')
                    AND NVL(T1.gaitou_up__c, '') = NVL(T2.gaitou_up__c, '')
                    AND NVL(T1.gaitiou_under_k__c, '') = NVL(T2.gaitiou_under_k__c, '')
                    AND NVL(T1.keisanti_under_p__c, '') = NVL(T2.keisanti_under_p__c, '')
                    AND NVL(T1.error_up_k__c, '') = NVL(T2.error_up_k__c, '')
                    AND NVL(T1.k_up__c, '') = NVL(T2.k_up__c, '')
                    AND NVL(T1.keiyakusyoukainashi_up_k__c, '') = NVL(T2.keiyakusyoukainashi_up_k__c, '')
                    AND NVL(T1.gaitoualert_k__c, '') = NVL(T2.gaitoualert_k__c, '')
                    AND NVL(T1.keiyaku_k_down__c, '') = NVL(T2.keiyaku_k_down__c, '')
                    AND NVL(T1.mikanalert_k_up__c, '') = NVL(T2.mikanalert_k_up__c, '')
                    AND NVL(T1.gainashiruinashi_k_down__c, '') = NVL(T2.gainashiruinashi_k_down__c, '')
                    AND NVL(T1.stpc3_k_up__c, '') = NVL(T2.stpc3_k_up__c, '')
                    AND NVL(T1.keisanti_p_miman__c, '') = NVL(T2.keisanti_p_miman__c, '')
                    AND NVL(T1.keisanti_p__c, '') = NVL(T2.keisanti_p__c, '')
);