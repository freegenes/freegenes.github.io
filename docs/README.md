# Freegenes

## Sheets Backend

## Shopify <> Sheets Integration

### Slack bots

#### "I MAKE HEATMAPS"

This bot makes heatmaps. Example:

![Heatmap Example (1)](./images/freegenes-heatmap-output1.png)
![Heatmap Example (2)](./images/freegenes-heatmap-output2.png)

#### "I MAKE BACKUPS"

This bot makes backups of the backend sheet.

This bot is run by two AWS Lambda functions - one (`freegenes-backup-init`) handles slash commands (`/backup`) from slack and the other (`freegenes-backup`) actually makes the backup (this is due to slack wanting a quick return too it's messages). The backup is saved as an `xlsx` excel file in an AWS S3 bucket (currently `freegenes-backups`) with a filename of `shopify-sheets.{timestamp}.{hash}.backup.xlsx` where `{timestamp}` is of the form `%Y-%m-%d_%H-%M-%S` (eg. `2020-01-29_13-30-30`) and `{hash}` is the first 8 chars of a sha1 hash (eg. `2b49af2d`). The `freegenes-backup` is run by an EventBridge recurring timer. At the time of writing, it's setup to run daily. Regardless of how the bot is triggered, it posts in the slack. 

![Backup Example (1)](./images/freegenes-backup-example1.png)

If it's triggered by a slack slash command (`/backup`), it will post who requested the backup: (along with a nice message to the requester!)

![Backup Example (2)](./images/freegenes-backup-example2.png)





Written by Gwyn. They can be found at [gwynu.dev](http://gwynu.dev)
