CREATE OR REPLACE PROCEDURE spr_MergeDimIndicators AS
BEGIN
    -- Truncate mediator table, temporary carrying outdated records
    EXECUTE IMMEDIATE 'TRUNCATE TABLE dim_IndicatorsOld DROP STORAGE';
    COMMIT;
    
    -- Merge new records from STG_INDICATORS to DIM_INDICATORS
    MERGE INTO dim_Indicators dim USING stg_indicators stg
    ON (stg.indicator_name = dim.indicatorname)
    WHEN MATCHED THEN UPDATE SET
        dim.MeasureUnits = TRIM(stg.measure_units),
        dim.Frequency    = TRIM(stg.frequency),
        dim.Category     = TRIM(stg.category),
        dim.FromDate     = TO_DATE(TRIM(start_date), 'MM YYYY'),
        dim.ToDate       = NULL,
        dim.IsCurrent    = 'Yes'
    WHERE 
        (dim.MeasureUnits <> TRIM(stg.measure_units) OR
        dim.Frequency     <> TRIM(stg.frequency) OR
        dim.Category      <> TRIM(stg.category) OR
        dim.FromDate      <> TO_DATE(TRIM(start_date), 'MM YYYY')) AND
        (dim.IsCurrent    = 'Yes')
    WHEN NOT MATCHED THEN INSERT(
        dim.IndicatorName, dim.MeasureUnits, dim.Frequency,
        dim.Category, dim.FromDate, dim.ToDate, dim.IsCurrent)
    VALUES(
        TRIM(stg.indicator_name), TRIM(stg.measure_units), TRIM(stg.frequency),
        TRIM(stg.category), TO_DATE(TRIM(start_date), 'MM YYYY'), NULL, 'Yes');   
    COMMIT;
    
    -- Merge new records from STG_INDICATORS to FACT_INDICATORS 
    -- ... using previously updated DIM_INDICATORS
    MERGE INTO FACT_INDICATORS fct USING (
    SELECT dim.IndicatorKey, dim.IndicatorID, stg.start_date,
           stg.frequency, stg.value 
    FROM STG_INDICATORS stg 
    INNER JOIN DIM_INDICATORS dim 
    ON TRIM(stg.indicator_name) = dim.IndicatorName
    WHERE dim.ISCURRENT = 'Yes'
    ) tmp
    ON (fct.IndicatorKey = tmp.IndicatorKey 
    AND fct.IndicatorID  = tmp.IndicatorID
    AND fct.FromDate     = TO_DATE(TRIM(tmp.start_date), 'MM YYYY'))
    WHEN MATCHED THEN UPDATE SET
        fct.ToDate         =  CASE tmp.frequency 
                              WHEN 'Daily' THEN TO_DATE(TRIM(tmp.start_date), 'MM YYYY') + 1
                              WHEN 'Monthly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 1)
                              WHEN 'Quarterly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 3)
                              WHEN 'Yearly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 12)
                              ELSE NULL END,
        fct.IndicatorValue  = CAST(tmp.value AS NUMERIC(14, 2))
    WHERE 
        (CAST(tmp.value AS NUMERIC(14, 2)) <> fct.IndicatorValue)
    WHEN NOT MATCHED THEN INSERT(
        fct.IndicatorKey, fct.IndicatorID, fct.FromDate, fct.ToDate, fct.IndicatorValue)
    VALUES(
        tmp.IndicatorKey, tmp.IndicatorID, TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 
        CASE tmp.frequency 
            WHEN 'Daily' THEN TO_DATE(TRIM(tmp.start_date), 'MM YYYY') + 1
            WHEN 'Monthly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 1)
            WHEN 'Quarterly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 3)
            WHEN 'Yearly' THEN ADD_MONTHS(TO_DATE(TRIM(tmp.start_date), 'MM YYYY'), 12)
            ELSE NULL END,
        CAST(tmp.value AS NUMERIC(14, 2)));
    COMMIT;
    
    -- Clean staging table
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_Indicators DROP STORAGE';
    COMMIT;
    
END;


-- Takes records updated w/ merge and puts them indo a mediator table, assigning the ToDate and IsCurrent
CREATE OR REPLACE TRIGGER trg_UpdateDimIndicators 
BEFORE UPDATE ON dim_Indicators FOR EACH ROW
    BEGIN
        INSERT INTO dim_IndicatorsOld(
            IndicatorID, IndicatorName, MeasureUnits, Frequency, 
            Category, FromDate, ToDate, IsCurrent)
        VALUES(
            :OLD.IndicatorID, :OLD.IndicatorName, :OLD.MeasureUnits, :OLD.Frequency, 
            :OLD.Category, :OLD.FromDate, (SELECT SYSDATE FROM DUAL), 'No');
    END trg_UpdateDimIndicators;  


-- Used to assign an IndicatorID for a new Indicator 
-- (since Oracle doesn't support 2 identity columns on 1 table)
CREATE SEQUENCE seq_IndicatorID 
	START WITH 1 INCREMENT BY 1    
	NOCACHE NOCYCLE NOMAXVALUE;


-- Used to assign an IndicatorID when new indicator is inserted
CREATE OR REPLACE TRIGGER trg_AssignIndicatorID
BEFORE INSERT ON dim_Indicators FOR EACH ROW
	BEGIN
        SELECT seq_IndicatorID.NEXTVAL
        INTO :NEW.IndicatorID
        FROM dual;
    END trg_AssignIndicatorID; 


-- Returns history records back to the dimension
CREATE OR REPLACE PROCEDURE spr_InsertDimOld AS
    BEGIN
        INSERT INTO dim_Indicators(
            IndicatorID, IndicatorName, MeasureUnits, Frequency, 
            Category, FromDate, ToDate, IsCurrent)
        SELECT
            IndicatorID, IndicatorName, MeasureUnits, Frequency, 
            Category, FromDate, ToDate, IsCurrent
        FROM dim_IndicatorsOld;
    END spr_InsertDimOld; 
       