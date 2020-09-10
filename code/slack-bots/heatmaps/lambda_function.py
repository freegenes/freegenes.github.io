import json
import seaborn as sns
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import io
import os
from slack import WebClient
from slack.errors import SlackApiError
import requests as r
from datetime import datetime


SPREADSHEET_ID = "1LZCXzBtgey9xv5OH7YGYgp8UMJ27Eyj1aF9IhAW6M6o"
client = WebClient(token=os.environ['SLACK_VERIFICATION_TOKEN'])
channel = os.environ['SLACK_CHANNEL']

def getSheet(ID, sheet):
    resp = r.get(f'https://docs.google.com/spreadsheet/ccc?key={ID}&output=csv')
    print(resp.status_code)
    NAs = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'NULL', 'NaN', 'nan', 'null']
    return pd.read_csv(io.BytesIO(resp.content), keep_default_na=False, na_values=NAs)

def uploadFile(f):
    try:
        filename = f"HEATMAP ({datetime.now().strftime('%Y-%m-%d-%H')}).png"
        response = client.files_upload(channels=channel, file=f, filename=filename, title=filename, filetype="PNG")
        assert response["file"]  # the uploaded file
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error: {e.response['error']}")

def lambda_handler(event, context):
    client.chat_postMessage(channel=channel,text="Generating heatmaps....")
    
    genes = getSheet(SPREADSHEET_ID, "Genes")

    prodGenes = genes.groupby("product").count()["id"].to_dict()
    
    perc = {prod: ((~genes[genes["product"]==prod].replace("", np.nan).isna()).sum()/prodGenes[prod]).to_dict() for prod in prodGenes}
    del perc["The name of the larger gene collection within which the part is contained"]
    df = pd.DataFrame(perc)*100
    for d, size in [(df.T, (20,7)), (df, (8, 20))]:
        with io.BytesIO() as iBuff:
            plt.figure(figsize=size)
            sns.heatmap(d.astype(int), center=50, cmap="RdYlGn", square=True, annot=True, fmt="d")
            plt.tight_layout()
            plt.savefig(iBuff, dpi=300)
            iBuff.seek(0)
            uploadFile(iBuff)
    return {
    'statusCode': 200,
    'body': "Done"
    }
