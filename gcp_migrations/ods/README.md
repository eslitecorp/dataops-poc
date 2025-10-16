
=> DAG-1: Health Check



## Choose the scheduled SSIS jobs, which outputs data to DM, as primary index
DAILY_STAGE_MAIN_POSNEC
- ./ods/mf/dept/building
    - view: VW_MF_DEPT_BUILDING
    - tables:
        - ods:  MF_DEPT_BUILDING
        - staging:  
            - tw:
                - DW.DimDeptsList
                - POSNEC.Store
                - POSNEC.BuildingM

- VW_MF_DEPT_WERKS
- VW_MF_POS_DISCOUNT
- VW_TRANS_DETAIL_DISCOUNT
- VW_TRANS_DETAIL_PACKAGE