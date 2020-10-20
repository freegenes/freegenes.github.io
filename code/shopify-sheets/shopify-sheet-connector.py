from utils import *
import re
import numpy as np
import shopify
from bleach.linkifier import Linker
from pyactiveresource.connection import ResourceNotFound

linkify = Linker().linkify
import shopifyLimitPatch
from pyactiveresource.connection import ResourceNotFound

SPREADSHEET_ID = "1LZCXzBtgey9xv5OH7YGYgp8UMJ27Eyj1aF9IhAW6M6o"


data = getShopifyProductData()
shopify.CarrierService.find()
df = productDataToDF(data)

getSheet, updateSheet, sheets, service = authenticateGS()

fedexInfo = getSheet(SPREADSHEET_ID, "FedEx Info", useFirstRowAsCols=True)
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

#dff = dff.applymap(lambda x: x[0:40000] if len(x)>40000 else x)
updateSheet(dff, SPREADSHEET_ID, "Product Information")

df = recombineSplitColumns(df)

packaging = getSheet(SPREADSHEET_ID, "Packaging", useFirstRowAsCols=True)
collections = getSheet(SPREADSHEET_ID, "Collections", useFirstRowAsCols=True)

for i, row in df.iterrows():
    try:
        product = shopify.Product.find(row["id"])
    except ResourceNotFound as e:
        print(f"!!! ERROR   !!! Could not find any matches for {row['title']} with an id of {row['id']} in packaging. Error: {e}")
        continue
    packages = packaging[packaging["id"].replace("", -1).astype(int) == int(row["id"])]
    if not len(packages) == 1:
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
        continue

    genes=[]
    for geneList in collectionGenes.to_list():
        genes.extend(geneList.split(", "))
    genes = sorted(list(set(genes)))
    if "" in genes:
        genes.remove("")

    allGeneInfo = getSheet(SPREADSHEET_ID, "Genes", useFirstRowAsCols=True)
    geneInfo = allGeneInfo[allGeneInfo["id"].isin(genes)]

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
    tradingCardGeneDf.applymap(str).applymap(linkify)

    for i, gene in tradingCardGeneDf.iterrows():
        geneHtml = template
        for col in allGeneInfo:
            value = gene[col]
            if value in ["None", None, ""]:
                value = "<span class='none'>No Value</span>"
            key = "{" + col + "}"
            while key in geneHtml:
                geneHtml = geneHtml.replace(key, value)  # not using built in format due to lacking partials
        with open("../../genes/{}.html".format(gene["id"]), "w") as f:
            f.write(geneHtml)

    geneStart = "<!--START:GENES-->"
    geneEnd = "<!--END:GENES-->"
    geneInfo["gene_name_short"] = "<a target='_freegenes' href='http://" + geneInfo["trading_card_link"] +"'>" + geneInfo["gene_name_short"] +"</a>"
    geneInfo = geneInfo[["gene_name_short", "gene_name_long", "genbank_protein_id"]]
    geneInfo.rename(columns={"gene_name_short":"Gene", "gene_name_long":"Name", "genbank_protein_id":"NCBI ID"}, inplace=True)
    
    geneInfo = geneInfo.replace("", np.nan).dropna(how="all", axis=1).dropna(how="all").fillna(value=np.nan)
    table = geneInfo.to_html(index=False, index_names=False, header=True, escape=False,
        na_rep="", formatters={"NCBI ID": lambda id: f"<a target='_NCBI' href='https://www.ncbi.nlm.nih.gov/protein/{id}'>{id}</a>"})

    product.body_html = re.sub(f"{geneStart}.*?{geneEnd}", geneStart+table+geneEnd, product.body_html, flags=re.DOTALL)
    status = product.save()
    if status:
        print(f"Pushed genes for product {row['title']}")
    else:
        print(f"Encountered issues when pushing genes for product {row['title']}")
        print("Status:", status)

