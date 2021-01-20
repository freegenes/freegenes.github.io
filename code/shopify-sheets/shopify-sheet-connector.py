from utils import *
import re
import numpy as np
import shopify
from bleach.linkifier import Linker
from pyactiveresource.connection import ResourceNotFound

linkify = Linker().linkify
import shopifyLimitPatch
from pyactiveresource.connection import ResourceNotFound

from slack import WebClient
from slack.errors import SlackApiError
import json

slackInfo = json.load(open("slack-token.json", "r"))
channel = slackInfo["channel"]
heatmap = slackInfo["heatmap"]
shutdown = slackInfo["shutdown"]
token = slackInfo["token"]

client = WebClient(token=token)


client.chat_postMessage(channel=channel, text=f":wave: Hi! I'm the freegenes wizard. Let's get to work!")

client.chat_postMessage(channel=channel, text=f"First, let me get you a heatmap...")
import requests as r
if heatmap:
    r.post("https://q123xg5ca6.execute-api.us-east-1.amazonaws.com/default/slack-bot-heatmap-generate?username=wizard") # heatmaps

pd.options.mode.chained_assignment = None  # default='warn'


SPREADSHEET_ID = "1LZCXzBtgey9xv5OH7YGYgp8UMJ27Eyj1aF9IhAW6M6o"


client.chat_postMessage(channel=channel, text=f"Next, let me pull data from Shopify... (this might take a while!)")
data = getShopifyProductData()
shopify.CarrierService.find()
df = productDataToDF(data)

getSheet, updateSheet, sheets, service = authenticateGS()

client.chat_postMessage(channel=channel, text=f"Now, let me get FedEx info from google sheets...")

fedexInfo = getSheet(SPREADSHEET_ID, "FedEx Info", useFirstRowAsCols=True)

client.chat_postMessage(channel=channel, text=f"Give me a moment to reformat the data.")

fedexInfo.rename(columns={"Handle": "handle", "Length": "length", "Width": "width", "Height": "height",
                          "Country Of Manufacture": "country_of_manufacture"}, inplace=True)
df = df.merge(fedexInfo[["length", "height", "width", "country_of_manufacture", "handle"]], right_on="handle",
              left_on="handle", how="left")

df.rename(columns={"body_html": "body"}, inplace=True)

addCols = ["body::template", "body::teaser", "body::what_is_it", "body::what_can_it_be_used_for",
           "body::more_information",
           "body::instructions_for_use", "body::license", "body::makeup_table", "creator::physical_product",
           "physical_product_type", "body::author", "components", "packaging", "number_of_packages"]
for col in addCols:
    df[col] = ""


# cells can only have 50000 chars in them
    df = splitColsViaCharMax(df, charMax=49999)

columnOrder = sorted(df.columns)
columnOrder.remove("title")
columnOrder.remove("handle")
columnOrder = ["title", "handle"] + columnOrder
df = df[columnOrder]

dff = df.applymap(str)

client.chat_postMessage(channel=channel, text=f"Okay! Let me push that to the google sheet for you :)")

#dff = dff.applymap(lambda x: x[0:40000] if len(x)>40000 else x)
updateSheet(dff, SPREADSHEET_ID, "Product Information")
client.chat_postMessage(channel=channel, text=f":muscle: Pushed!")


df = recombineSplitColumns(df)

client.chat_postMessage(channel=channel, text=f"Let me get more data, starting with Packaging")
packaging = getSheet(SPREADSHEET_ID, "Packaging", useFirstRowAsCols=True)
client.chat_postMessage(channel=channel, text=f"And the Collections...")
collections = getSheet(SPREADSHEET_ID, "Collections", useFirstRowAsCols=True)
client.chat_postMessage(channel=channel, text=f"And the Genes...")
allGeneInfo = getSheet(SPREADSHEET_ID, "Genes", useFirstRowAsCols=True)

client.chat_postMessage(channel=channel, text=f"Now that I have _all that data_, let me do something useful. How about I make some tables? ;)")

for i, row in df.iterrows():
    try:
        product = shopify.Product.find(row["id"])
    except ResourceNotFound as e:
        print(f"!!! ERROR   !!! Could not find any matches for {row['title']} with an id of {row['id']} in packaging. Error: {e}")
        client.chat_postMessage(channel=channel, text=f":x: Could not find any matches for {row['title']} with an id of {row['id']} in packaging. Error: {e}")
        continue
    packages = packaging[packaging["id"].replace("", -1).astype(int) == int(row["id"])]
    if not len(packages) == 1:
        client.chat_postMessage(channel=channel, text=f":warning: There are {len(packages)} matches for {row['title']} with an id of {row['id']} in packaging.")
        print(f"!!! WARNING !!! There are {len(packages)} matches for {row['title']} with an id of {row['id']} in packaging.")
        continue
    package = packages.iloc[0]
    components = package["composition_collections"].split(", ")
    if " " in components:
        components.remove(" ")
    if "" in components:
        components.remove("")
    collectionGenes = collections[collections["name"].isin(components)]["composition_genes"]
    if not len(components)==len(collectionGenes):
        print(f"!!! WARNING !!! Matches were not found for all collections of package {row['handle']}")
        print(components, collectionGenes)
        client.chat_postMessage(channel=channel, text=f":warning: Matches were not found for all collections of package {row['handle']}")
        client.chat_postMessage(channel=channel, text=f":warning: {components} {collectionGenes}")
        continue

    genes=[]
    for geneList in collectionGenes.to_list():
        genes.extend(geneList.split(", "))
    genes = sorted(list(set(genes)))
    if "" in genes:
        genes.remove("")

    geneInfo = allGeneInfo[allGeneInfo["id"].isin(genes)]


    geneStart = "<!--START:GENES-->"
    geneEnd = "<!--END:GENES-->"
    for i, gene in geneInfo.iterrows():
        if not pd.isna(gene["trading_card_link"]):
            gene["gene_name_short"] = "<a target='_freegenes' href='http://" + gene["trading_card_link"] +"' style='cursor:pointer;' title='"+gene["description"]+"'>" + \
                                 gene.fillna("")["gene_name_short"] + "</a>"
    geneInfo = geneInfo[["gene_name_short", "gene_name_long", "genbank_protein_id"]]
    geneInfo.rename(columns={"gene_name_short": "Gene", "gene_name_long": "Name", "genbank_protein_id": "NCBI ID"}, inplace=True)
    
    geneInfo = geneInfo.replace("", np.nan).dropna(how="all", axis=1).dropna(how="all").fillna(value=np.nan)
    table = geneInfo.to_html(index=False, index_names=False, header=True, escape=False,
        na_rep="", formatters={"NCBI ID": lambda id: f"<a target='_NCBI' href='https://www.ncbi.nlm.nih.gov/protein/{id}'>{id}</a>"})

    product.body_html = re.sub(f"{geneStart}.*?{geneEnd}", geneStart+table+geneEnd, product.body_html, flags=re.DOTALL)
    status = product.save()
    if status:
        print(f"Pushed genes for product {row['title']}")
        client.chat_postMessage(channel=channel, text=f":ok: Pushed genes for product {row['title']}")
    else:
        print(f"Encountered issues when pushing genes for product {row['title']}")
        print("Status:", status)        
        client.chat_postMessage(channel=channel, text=f":x: Encountered issues when pushing genes for product {row['title']}")
        client.chat_postMessage(channel=channel, text=f":x: Status: {status}")
with open("genes-template.html", "r") as f:
    template = f.read()

tradingCardGeneDf = allGeneInfo.iloc[1:]
tradingCardGeneDf = tradingCardGeneDf.applymap(str)

def canaryFormatter(cValue):
    text = "We are unaware of third-party property rights claims on uses of this item"
    if cValue == "False":
        return ""
    if not cValue == "True":
        cValue = cValue.split(" ")[0]
        text = f"{text} as of {cValue}"
    text = text + "."
    return text

tradingCardGeneDf["canary_notice"] = tradingCardGeneDf["canary_notice"].apply(canaryFormatter)
tradingCardGeneDf["uniprot_link"] = tradingCardGeneDf["uniprot_link"].apply(lambda x: x.replace("https://www.uniprot.org/uniprot/", ""))
print("Generating trading cards... :baseball:")
client.chat_postMessage(channel=channel, text="Generating trading cards... :baseball:")
for i, gene in tradingCardGeneDf.iterrows():
    geneHtml = template
    for col in allGeneInfo:
        value = gene[col]
        if value in ["None", None, ""]:
            value = "<span class='none'>No Value</span>"
        key = "{" + col + "}"
        while key in geneHtml:
            geneHtml = geneHtml.replace(key, value if ((col in ["documentation_image", "genbank_file_link"]) or "link" in col) else linkify(value))  # not using built in format due to lacking partials
    geneHtml = geneHtml.replace('<a target="_NCBI" href="<span class=\'none\'>No Value</span>"><span class="none">No Value</span></a>', '<span class="none">No Value</span>')
    with open("../../genes/{}.html".format(gene["id"]), "w") as f:
        f.write(geneHtml)

client.chat_postMessage(channel=channel, text=f"Pushing to github... :page_with_curl:")
os.system('git add ../../genes && git commit -m "Auto update to genes." && eval "$(ssh-agent -s)" && ssh-add ~/.ssh/github && git push')

client.chat_postMessage(channel=channel, text=f"Done! :heart:")
if shutdown:
    os.system("sudo shutdown -h now")

