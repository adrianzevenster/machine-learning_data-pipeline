create
    definer = root@localhost procedure GetDailyCDRDataBatch(IN startDate date)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE currentDate DATE;

    -- Cursor to iterate over the distinct dates
    DECLARE dateCursor CURSOR FOR
    SELECT DISTINCT DATE(DP_DATE)
    FROM RawData.DP_CDR_Data
    WHERE DATE(DP_DATE) >= startDate AND DATE(DP_DATE) <= CURDATE();

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- Create the DailySummary table if it doesn't exist
    CREATE TABLE IF NOT EXISTS RawData.DailySummary (
        Date DATE,
        Total_Outgoing_Calls INT,
        Total_Outgoing_Call_Time INT,
        Total_Data_Purchases INT,
        Total_Data_Volume DECIMAL(10, 2)
    );

    OPEN dateCursor;

    date_loop: LOOP
        FETCH dateCursor INTO currentDate;

        IF done = 1 THEN
            LEAVE date_loop;
        END IF;

        -- Aggregate data for the current date
        INSERT INTO RawData.DailySummary (Date, Total_Outgoing_Calls, Total_Outgoing_Call_Time, Total_Data_Purchases, Total_Data_Volume)
        SELECT
            currentDate AS Date,
            SUM(DP_MOC_COUNT) AS Total_Outgoing_Calls,
            SUM(DP_MOC_DURATION) AS Total_Outgoing_Call_Time,
            SUM(DP_DATA_COUNT) AS Total_Data_Purchases,
            SUM(DP_DATA_VOLUME) AS Total_Data_Volume
        FROM RawData.DP_CDR_Data
        WHERE DATE(DP_DATE) = currentDate;
    END LOOP;

    CLOSE dateCursor;

    SELECT * FROM RawData.DailySummary WHERE Date >= startDate AND Date <= CURDATE();
END;

