/*
Run on AHL / Prodoc2021 before starting the HID radiology webhook worker.
The worker also runs this migration automatically, but keeping the script here
makes deployment and review easier.
*/

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookStatus') IS NULL
BEGIN
    ALTER TABLE dbo.RadiologyOrderList
    ADD RadiologyWebhookStatus TINYINT NULL;

    /*
    Existing rows are marked as sent so deployment does not bulk-post old
    historical orders. New inserts receive default 0 and are posted.
    */
    EXEC('UPDATE dbo.RadiologyOrderList
          SET RadiologyWebhookStatus = 2
          WHERE RadiologyWebhookStatus IS NULL');

    ALTER TABLE dbo.RadiologyOrderList
    ADD CONSTRAINT DF_RadiologyOrderList_RadiologyWebhookStatus DEFAULT (0)
    FOR RadiologyWebhookStatus;
END;

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookAttempts') IS NULL
    ALTER TABLE dbo.RadiologyOrderList
    ADD RadiologyWebhookAttempts INT NOT NULL
        CONSTRAINT DF_RadiologyOrderList_RadiologyWebhookAttempts DEFAULT (0);

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookLastAttemptOn') IS NULL
    ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookLastAttemptOn DATETIME NULL;

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookSentOn') IS NULL
    ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookSentOn DATETIME NULL;

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookResponseCode') IS NULL
    ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookResponseCode INT NULL;

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookResponse') IS NULL
    ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookResponse NVARCHAR(1000) NULL;

IF COL_LENGTH('dbo.RadiologyOrderList', 'RadiologyWebhookVisitId') IS NULL
    ALTER TABLE dbo.RadiologyOrderList ADD RadiologyWebhookVisitId INT NULL;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.RadiologyOrderList')
      AND name = 'IX_RadiologyOrderList_RadiologyWebhookQueue'
)
BEGIN
    CREATE INDEX IX_RadiologyOrderList_RadiologyWebhookQueue
    ON dbo.RadiologyOrderList
        (RadiologyWebhookStatus, RadiologyWebhookAttempts, RadiologyWebhookLastAttemptOn, Id);
END;

/*
Status values:
0 = pending
1 = processing
2 = sent
3 = failed/retryable until max attempts
*/
