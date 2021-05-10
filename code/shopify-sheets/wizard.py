from utils import *
import re, os
import numpy as np
import shopify
from bleach.linkifier import Linker
from pyactiveresource.connection import ResourceNotFound
from tqdm import tqdm
import time
from hashlib import sha1
import socket
from datetime import datetime
from io import BytesIO, StringIO

yesPar = {'name': 'Participate in Bionet', 'value': 'Yes'}
noPar = {'name': 'Participate in Bionet', 'value': 'No'}
blankContact = {'name': 'Bionet Contact', 'value': 'NA'}

pd.options.mode.chained_assignment = None  # default='warn'

linkify = Linker().linkify
import shopifyLimitPatch
from pyactiveresource.connection import ResourceNotFound

from slack import WebClient
from slack.errors import SlackApiError
import json
import requests as r

SPREADSHEET_ID = "1LZCXzBtgey9xv5OH7YGYgp8UMJ27Eyj1aF9IhAW6M6o"
slackInfo = json.load(open("slack-token.json", "r"))
channel = slackInfo["channel"]
generateHeatmap = slackInfo["heatmap"]
shutdown = slackInfo["shutdown"]
token = slackInfo["token"]

client = WebClient(token=token)
getSheet, updateSheet, sheets, service = authenticateGS()

def initilize(client):
    client.chat_postMessage(channel=channel, text=f":wave: Hi! I'm the freegenes wizard. Let's get to work!")
    client.chat_postMessage(channel=channel, text=f"Getting settings from 'Wizard Settings' sheet...")

    settings = getSheet(SPREADSHEET_ID, "Wizard Settings", useFirstRowAsCols=True)
    d = settings.to_dict()
    settings = {d["Option"][i]: d["Value"][i] if not d["Value"][i] in ["TRUE", "FALSE"] else bool(d["Value"][i]) for i in d["Option"].keys()}
    return settings

def heatmap(client):
    client.chat_postMessage(channel=channel, text=f"First, let me get you a heatmap...")
    r.post("https://q123xg5ca6.execute-api.us-east-1.amazonaws.com/default/slack-bot-heatmap-generate?username=wizard")  # heatmaps

def getShopifyData(client, shopify):
    client.chat_postMessage(channel=channel, text=f"Next, let me pull data from Shopify... (this might take a while!)")
    data = getShopifyProductData()
    shopify.CarrierService.find()
    df = productDataToDF(data)
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
    return df

def pushShopifyDataToSheet(client, df):
    dff = splitColsViaCharMax(df, charMax=49999)

    columnOrder = sorted(df.columns)
    columnOrder = ["title", "handle"] + columnOrder
    dff = dff[columnOrder]
    dff = dff.applymap(str)

    client.chat_postMessage(channel=channel, text=f"Okay! Let me push that to the google sheet for you :)")

    updateSheet(dff, SPREADSHEET_ID, "Product Information")
    updateSheet(dff, SPREADSHEET_ID, "Product Information")
    client.chat_postMessage(channel=channel, text=f":muscle: Pushed!")

def getDataFromBackEnd(client):
    client.chat_postMessage(channel=channel, text=f"Let me get more data, starting with Packaging")
    packaging = getSheet(SPREADSHEET_ID, "Packaging", useFirstRowAsCols=True)
    client.chat_postMessage(channel=channel, text=f"And the Collections...")
    collections = getSheet(SPREADSHEET_ID, "Collections", useFirstRowAsCols=True)
    client.chat_postMessage(channel=channel, text=f"And the Genes...")
    genes = getSheet(SPREADSHEET_ID, "Genes", useFirstRowAsCols=True)
    client.chat_postMessage(channel=channel, text=f"And all of the orders...")
    orders = getAllShopifyOrders(fulfillment_status="fulfilled")
    return packaging, collections, genes, orders


def weaveBionet(client, orders):
    for o in orders:
        c = False
        fields = [a.to_dict()["name"] for a in o.note_attributes]
        if not noPar["name"] in fields:
            o.note_attributes.append(shopify.NoteAttribute(noPar))
            c = True
        if not blankContact["name"] in fields:
            c = True
            o.note_attributes.append(shopify.NoteAttribute(blankContact))
        if c:
            o.save()
            time.sleep(1)
    client.chat_postMessage(channel=channel, text=f"Weaving the bionet... :ringed_planet:")
    bionet = {}
    for o in orders:
        fields = {a.to_dict()["name"]: a.to_dict()["value"] for a in o.note_attributes}
        if fields[noPar["name"]] == "Yes" and o.attributes["fulfillment_status"] == "fulfilled":
            info = (o.attributes["customer"].attributes["first_name"], o.attributes["customer"].attributes["last_name"],
                    fields["Bionet Contact"])
            for l in o.line_items:
                if not l.attributes["product_id"] in bionet.keys():
                    bionet[l.attributes["product_id"]] = []
                bionet[l.attributes["product_id"]] = list(set(bionet[l.attributes["product_id"]] + [info]))
    return bionet


def updateGeneRowHashes(client, genes):
    hashRows = lambda row: sha1("---".join([str(row[x]) for x in genes.columns if not x=="row-hash"]).encode()).hexdigest()[0:8]
    pushGeneInfo = genes.copy()
    pushGeneInfo["row-hash"] = genes.apply(hashRows, axis=1)

    try:
        updateSheet(pushGeneInfo.applymap(str).replace("None", ""), SPREADSHEET_ID, "Genes")
    except socket.timeout:
        print(":alarm_clock::alarm_clock::alarm_clock: Could not push gene info to 'Genes' table; likely due to a poor connection")
        client.chat_postMessage(channel=channel, text=f"!!!WARNING!!! Could not push gene info to 'Genes' table; likely due to a poor connection")
    return pushGeneInfo["row-hash"]


def canaryFormatter(cValue, product_level=False):
    if product_level:
        if cValue:
            return "We are unaware of any third-party property rights claims on uses of these items."
        else:
            return "Some or all of these items are for use only as permitted by a research exemption."
    text = "We are unaware of third-party property rights claims on uses of this item"
    if cValue == "False":
        return ""
    if not cValue == "True":
        cValue = cValue.split(" ")[0]
        text = f"{text} as of {cValue}"
    text = text + "."
    return text

def generateTradingCards(client, genes):
    with open("genes-template.html", "r") as f:
        template = f.read()

    tradingCardGeneDf = genes.iloc[1:]
    tradingCardGeneDf = tradingCardGeneDf.applymap(str)

    tradingCardGeneDf["canary_notice"] = tradingCardGeneDf["canary_notice"].apply(canaryFormatter)
    tradingCardGeneDf["uniprot_link"] = tradingCardGeneDf["uniprot_link"].apply(
        lambda x: x.replace("https://www.uniprot.org/uniprot/", ""))
    print("Generating trading cards... :baseball:")
    client.chat_postMessage(channel=channel, text="Generating trading cards... :baseball:")
    for i, gene in tradingCardGeneDf.iterrows():
        geneHtml = template
        for col in genes:
            if col in ["None", None, ""] or pd.isna(col):
                continue
            try:
                value = gene[col]
                if pd.isna(value) or value in ["None", None, ""]:
                    value = "<span class='none'>No Value</span>"
                key = "{" + col + "}"
                while key in geneHtml:
                    geneHtml = geneHtml.replace(key, value if (
                            (col in ["documentation_image", "genbank_file_link"]) or "link" in col) else linkify(
                        value))  # not using built in format due to lacking partials
            except Exception as e:
                client.chat_postMessage(channel=channel, text=f":exclamation: Error generating baseball card {gene['id']}, column {col} raised error {e.__class__.__name__}")
                print(f"Error generating baseball card {gene['id']}, column {col} raised error {e.__class__.__name__}")
        geneHtml = geneHtml.replace(
            '<a target="_NCBI" href="<span class=\'none\'>No Value</span>"><span class="none">No Value</span></a>',
            '<span class="none">No Value</span>')
        with open("../../genes/{}.html".format(gene["id"]), "w") as f:
            f.write(geneHtml)
        if not os.path.isfile(f"../../genbank/{gene['id']}.gb"):
            pass
            #client.chat_postMessage(text=f":interrobang: Writing {gene['id']}.html but can't find a corresponding genbank file! (until this is fixed, there will be no image on the trading card", channel=channel)
            print(f"ERROR: Writing {gene['id']}.html but can't find a corresponding genbank file! (until this is fixed, there will be no image on the trading card")

def generateSnapgeneImages(client, changed, genes):
    genbanks = os.listdir("./../../genbank")
    if changed:
        client.chat_postMessage(channel=channel, text=f"Creating snapgene images for genes that have been edited...")
        genbanks = list(filter(lambda gb: any([id in gb for id in genes[genes["changed"]]["id"]]), genbanks))
    else:
        client.chat_postMessage(channel=channel, text=f"Creating all snapgene images... (this takes a while (often >1h))")
    for gb in tqdm(genbanks):
        if ".gb" in gb:
            os.system('/opt/gslbiotech/snapgene-server/snapgene-server.sh --command \'{"request": "generatePNGMap", "inputFile": "/home/ubuntu/freegenes/genbank/'+gb+'", "outputPng": "/home/ubuntu/freegenes/genes/images/'+gb.replace(".gb", ".png")+'"}\' > ~/snapgene.log 2>&1')
    os.system("rm -rf tmp_files")


def getProductGenes(client, shopifyData, collections, packaging):
    productGenes = []
    for i, row in shopifyData.iterrows():
        try:
            product = shopify.Product.find(row["id"])
        except ResourceNotFound as e:
            print(
                f"!!! ERROR   !!! Could not find any matches for {row['title']} with an id of {row['id']} in packaging. Error: {e}")
            client.chat_postMessage(channel=channel,
                                    text=f":x: Could not find any matches for {row['title']} with an id of {row['id']} in packaging. Error: {e}")
            raise e
        packages = packaging[packaging["id"].replace("", -1).astype(int) == int(row["id"])]
        if not len(packages) == 1:
            client.chat_postMessage(channel=channel,
                                    text=f":warning: There are {len(packages)} matches for {row['title']} with an id of {row['id']} in packaging.")
            print(
                f"!!! WARNING !!! There are {len(packages)} matches for {row['title']} with an id of {row['id']} in packaging.")
            continue
        package = packages.iloc[0]
        components = package["composition_collections"].split(", ")
        if " " in components:
            components.remove(" ")
        if "" in components:
            components.remove("")
        collectionGenes = collections[collections["name"].isin(components)]["composition_genes"]
        if not len(components) == len(collectionGenes):
            print(f"!!! WARNING !!! Matches were not found for all collections of package {row['handle']}")
            print(components, collectionGenes)
            client.chat_postMessage(channel=channel,
                                    text=f":warning: Matches were not found for all collections of package {row['handle']}")
            client.chat_postMessage(channel=channel, text=f":warning: {components} {collectionGenes}")
            continue
        genes = []
        for geneList in collectionGenes.to_list():
            genes.extend(geneList.split(", "))
        genes = sorted(list(set(genes)))
        if "" in genes:
            genes.remove("")

        productGenes.append((product, genes))
    return productGenes


def updateGeneTableForProduct(client, shopify, product, productGenes):
    geneStart = "<!--START:GENES-->"
    geneEnd = "<!--END:GENES-->"
    productGenes=productGenes.copy()
    for i, gene in productGenes.iterrows():
        g = productGenes.loc[i].fillna("")
        print(productGenes.loc[i, "gene_name_short"])
        print("<a target='_freegenes' href='http://freegenes.github.io/genes/"+g["id"]+".html" + \
                                  "' style='cursor:pointer;' title='" + g["description"] + "'>" + \
                                  g["gene_name_short"] + "</a>")
        productGenes.loc[i, "gene_name_short"] = "<a target='_freegenes' href='http://freegenes.github.io/genes/"+g["id"]+".html" + \
                                  "' style='cursor:pointer;' title='" + g["description"] + "'>" + \
                                  g["gene_name_short"] + "</a>"
        print(productGenes.loc[i, "gene_name_short"])
    productGenes = productGenes[["gene_name_short", "gene_name_long", "genbank_protein_id", "id"]]
    productGenes.rename(columns={"gene_name_short": "Gene", "gene_name_long": "Name", "genbank_protein_id": "NCBI ID", "id": "Freegenes ID"},
                        inplace=True)

    productGenes = productGenes[[x for x in productGenes.columns if not x is None]]
    productGenes = productGenes.replace("", np.nan).dropna(how="all", axis=1).dropna(how="all").fillna(value=np.nan)
    table = productGenes.to_html(index=False, index_names=False, header=True, escape=False,
                                 na_rep="", formatters={
            "NCBI ID": lambda id: f"<a target='_NCBI' href='https://www.ncbi.nlm.nih.gov/protein/{id}'>{id}</a>"})

    product.body_html = re.sub(f"{geneStart}.*?{geneEnd}", geneStart + table + geneEnd, product.body_html,
                               flags=re.DOTALL)
    return product


def updateCanaryFlagForProduct(client, shopify, product, productGenes):
    canaryStart = "<!--START:CANARIA-->"
    canaryEnd = "<!--END:CANARIA-->"

    canaryStatus = set(productGenes.canary_notice) == {"False"}  # aka we don't have any flagged
    canary = canaryFormatter(canaryStatus, product_level=True)

    product.body_html = re.sub(f"{canaryStart}.*?{canaryEnd}", canaryStart + canary
                               + canaryEnd,
                               product.body_html, flags=re.DOTALL)
    return product


def updateBionetTableForProduct(client, shopify, product, productGenes):
    bionetStart = "<!--START:BIONET_DISTS-->"
    bionetEnd = "<!--END:BIONET_DISTS-->"

    bionetText = "<p>The bionet enables open peer-peer exchange of functional biomaterials and associated data.</p>" + \
                 "<p>This product may also be available from bionet nodes that are more convenient to you.</p>"

    #print("ID: : :", int(product["id"]))
    if int(product.id) in bionet.keys():
        df = pd.DataFrame(bionet[int(product.id)], columns=["fname", "lname", "Contact"])
        df["Name"] = df.fname + " " + df.lname
        df = df.drop(columns=["fname", "lname"])[["Name", "Contact"]]

        def link(x):
            if "@" in x:
                x = x.split("@")
                x[1] = x[1].replace(".", " {dot} ")
                return " {at} ".join(x)
            else:
                return f"<a href='{x}'>{x}</a>"

        df.Contact = df.Contact.apply(link)
        text = df.to_html(index=False, index_names=False, header=True, escape=False)
        #print(f"Found other bionet nodes for {row['title']}")
        bionetText = bionetStart + text
    else:
        bionetText = bionetStart + "<p>At the moment we are not aware of any other bionet nodes that provide this specific product.</p>"

    product.body_html = re.sub(f"{bionetStart}.*?{bionetEnd}", bionetStart + bionetText
                               + bionetEnd,
                               product.body_html, flags=re.DOTALL)
    return product


def updateProductPage(client, product):
    status = product.save()
    if status:
        print(f"Pushed genes, bionet tab and canary notice for product {product.title}")
        client.chat_postMessage(channel=channel, text=f":ok: Pushed genes, bionet tab and canary notice for product {product.title}")
    else:
        print(f"Encountered issues when pushing genes, bionet tab and/or canary notice for product {row['title']}")
        print("Status:", status)
        client.chat_postMessage(channel=channel,
                                text=f":x: Encountered issues when pushing genes, bionet tab and/or canary notice for product {row['title']}")
        client.chat_postMessage(channel=channel, text=f":x: Status: {status}")


def generateAndUploadOrdersReport(client, orders):
    client.chat_postMessage(channel=channel, text=f":eye: :eye: Generating visual compliance report...")
    today = datetime.today()
    df = pd.DataFrame([o.attributes for o in orders])
    df["shipping_address"] = df["shipping_address"].apply(lambda x: x.attributes)
    for key in df["shipping_address"].iloc[0].keys():
        df["shipping_"+key] = df["shipping_address"].apply(lambda x: x[key])
    df["items_shipped"] = [", ".join([x.attributes["title"] for x in y]) for y in df["line_items"]]
    sf = df[df["fulfillment_status"] == "fulfilled"][["id", "email", "closed_at", "fulfillment_status", "items_shipped"]+[x for x in df.columns if "shipping" in x and x not in ["total_shipping_price_set", "shipping_lines", "shipping_address"]]].copy()
    sf["closed_at"] = pd.to_datetime(sf["closed_at"], utc=True)
    export = sf[sf["closed_at"].dt.month == (today.month-1)].copy()
    export['closed_at'] = export['closed_at'].apply(lambda a: pd.to_datetime(a).date())

    date = datetime(today.year, today.month-1, 1)
    date = date.strftime("%B-%Y")
    filename = f"{date}-orders-report.csv"

    with open(filename, "w") as f:
        export.to_csv(f)

    response = client.files_upload(channels=channel, file=filename, filename=filename, title=filename, filetype="csv")
    assert response["file"]  # the uploaded file
    return



settings = initilize(client)
print("Settings:", settings)

if generateHeatmap:
    heatmap(client)

shopifyData = getShopifyData(client, shopify)
pushShopifyDataToSheet(client, shopifyData)
packaging, collections, genes, orders = getDataFromBackEnd(client)

if settings["Generate Visual Compliance Report"]:
    generateAndUploadOrdersReport(client, orders)

client.chat_postMessage(channel=channel, text=f"(and while we're here, let's autofill some note attributes)")

newHashes = updateGeneRowHashes(client, genes)
genes["changed"] = ~(newHashes == genes["row-hash"])
print(genes["changed"].unique())

bionet = weaveBionet(client, orders)

client.chat_postMessage(channel=channel,
                        text=f"Now that I have _all that data_, let me do something useful. How about I make some tables? ;)")

productGenes = getProductGenes(client, shopifyData, collections, packaging)
for product, geneList in productGenes:
    updatedProduct = product
    for func in [updateGeneTableForProduct, updateBionetTableForProduct, updateCanaryFlagForProduct, updateBionetTableForProduct]:
        updatedProduct = func(client, shopify, updatedProduct, genes[genes.id.isin(geneList)])
    updateProductPage(client, updatedProduct)


generateTradingCards(client, genes)
if settings["Generate Snapgene Images"] in ["TRUE", True, "Changed"]:
    generateSnapgeneImages(client, settings["Generate Snapgene Images"] == "Changed", genes)


client.chat_postMessage(channel=channel, text=f"Pushing to github... :page_with_curl:")
os.system(
    'git add ../../genes && git commit -m "Auto update to genes." && eval "$(ssh-agent -s)" && ssh-add ~/.ssh/github && git push')

client.chat_postMessage(channel=channel, text=f"Done! :heart:")
if shutdown:
    os.system("sudo shutdown -h now")
