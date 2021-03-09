import shopify
import pickle
from time import sleep
import json, urllib
import pyactiveresource
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd

SPREADSHEET_ID = "1LZCXzBtgey9xv5OH7YGYgp8UMJ27Eyj1aF9IhAW6M6o"


def getGenes(getSheet):
    genes = getSheet(SPREADSHEET_ID, "Genes", useFirstRowAsCols=True)
    return genes


def authenticateGS():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials-gsheets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets()

    # curry up some yummy functions....
    def getSheetFunc(sheetID, worksheetName, updateRange="A1:ZZZ10000", useFirstRowAsCols=False):
        # can't specify "all" so by default ask for a *very* large area, gsheets will automagically reduce area returned
        result = sheets.values().get(spreadsheetId=sheetID, range=f"{worksheetName}!{updateRange}", ).execute()
        df = pd.DataFrame(result["values"])
        if useFirstRowAsCols:
            df.columns = df.iloc[0]
            df.drop(df.index[0], inplace=True)
        if not result["majorDimension"] == "ROWS":
            df = df.T
        return df

    def updateSheetFunc(dataframe, sheetID, worksheetName, updateRange="A1:ZZZ10000", shiftColumnTitles=True):
        if shiftColumnTitles:
            dataframe = dataframe.shift(1, axis=0)
            dataframe.iloc[0] = dataframe.columns
        dataframe.fillna("", inplace=True)
        updateList = dataframe.values.tolist()
        updateRange = f"{worksheetName}!{updateRange}"
        r = sheets.values().update(spreadsheetId=sheetID, range=updateRange,
                                   valueInputOption="RAW",
                                   body={
                                       "values": updateList,
                                       "majorDimension": "ROWS",
                                   }).execute()

    return getSheetFunc, updateSheetFunc, sheets, service


def authenticateShopify():
    with open("credentials-shopify.json") as f:
        creds = json.load(f)

    # work around for shopify bug from :
    # https://github.com/Shopify/shopify_python_api/issues/314#issuecomment-487330640
    shopify.ShopifyResource.set_user(creds["api-key"])
    shopify.ShopifyResource.set_password(creds["password"])
    shopURL = "https://stanford-freegenes-org.myshopify.com/admin"
    shopify.ShopifyResource.set_site(shopURL)
    return shopify.Shop.current()


authenticateShopify()


def getShopifyProductData():
    metafieldsToTrack = ["Rack", "harmonized_system_code"]

    data = {}
    data["products"] = {}
    productProps = ["id", "body_html", "created_at", "handle", "options", "product_type",
                    "published_at", "tags", "title", "updated_at", "vendor"]  # , "images",]

    # options, variants, images
    for product in shopify.Product.find():
        data["products"][product.id] = {}

        for prop in productProps:
            if prop not in ["options", "variants"]:
                data["products"][product.id][prop] = getattr(product, prop)

        optionProps = ["id", "name", "position", "values"]
        for i, option in enumerate(product.options):
            for prop in optionProps:
                data["products"][product.id][f"option::{i}::{prop}"] = getattr(option, prop)

        variantVals = ['id', 'title', 'price',
                       'compare_at_price', 'grams', 'requires_shipping', 'sku', 'barcode',
                       'taxable', 'position', 'inventory_policy',
                       'inventory_quantity', 'inventory_management', 'fulfillment_service',
                       'weight', 'weight_unit', 'image_id', 'created_at', 'updated_at']  # ,
        # 'option_values'] - removed bc always blank WARNING might be needed in future
        for i, variant in enumerate(product.variants):
            for prop in variantVals:
                data["products"][product.id][f"variant::{i}::{prop}"] = getattr(variant, prop)
            for metafield in variant.metafields():
                if metafield.key in metafieldsToTrack:
                    data["products"][product.id][f"variant::{i}::{metafield.key}"] = metafield.value
                else:
                    print("Skipping variant metafield:", metafield.key)
                sleep(0.5)

        for metafield in product.metafields():
            if metafield.key in metafieldsToTrack:
                data["products"][product.id][f"metafeild::{metafield.key}"] = metafield.value
            else:
                print("Skipping product metafield:", metafield.key)
    return data


def recursivelyGetKeys(dic, label=None):
    returnKeys = set()
    for key in dic:
        returnKeys.add(key)
    return returnKeys


def joinSets(listOfSets):
    returnSet = set()
    for inputSet in listOfSets:
        returnSet = returnSet.union(inputSet)
    return returnSet


def productDataToDF(shopifyProductData):
    columns = [recursivelyGetKeys(shopifyProductData["products"][product]) for product in
               shopifyProductData["products"]]
    columns = sorted(list(joinSets(columns)))
    columns.remove("title")
    columns = ["title"] + columns
    flatData = {}
    for col in columns:
        columnArray = []
        for product in shopifyProductData["products"]:
            if col in shopifyProductData["products"][product]:
                columnArray.append(shopifyProductData["products"][product][col])
            else:
                columnArray.append(None)
        flatData[col] = columnArray
    return pd.DataFrame(flatData)


def pickleCache(cachedFunction):
    filename = f"{cachedFunction.__name__}.cache.pkl"
    try:
        cache = pickle.load(open(filename, "rb"))
    except (IOError, ValueError):
        cache = {}

    def f(*args, **kwargs):
        if args not in cache:
            cache[args] = cachedFunction(*args, **kwargs)
            pickle.dump(cache, open(filename, "wb"))
        return cache[args]

    return f


contString = "(cont.)"


def splitColsViaCharMax(df, charMax):
    dff = df.copy()
    for i, row in dff.iterrows():
        for column in dff.columns:
            if len(str(row[column])) > charMax:
                df.loc[i, column] = str(row[column])[0:charMax - 1]
                newCol = column + contString
                if not newCol in df.columns:
                    df[newCol] = ""
                df.loc[i, newCol] = str(row[column])[charMax - 1:]
    return df


def recombineSplitColumns(df):
    contCols = list(filter(lambda x: contString in x, df.columns))
    for col in contCols:
        origCol = col.replace(contString, "")
        df[origCol] = df[origCol] + df[col]
        df.drop(columns=col, inplace=True)
    return df


def getAllShopifyOrders(before=False, after=False, status="any", limit=250, fulfillment_status=None):
    minDate = lambda orders: min(orders, key=lambda o: o.id).attributes["created_at"]
    maxDate = lambda orders: max(orders, key=lambda o: o.id).attributes["created_at"]
    try:
        if before:
            orders = shopify.Order.find(status=status, created_at_max=before, limit=limit, fulfillment_status=fulfillment_status)
            if len(orders) > 1:
                orders.extend(getAllShopifyOrders(before=minDate(orders), status=status, limit=limit, fulfillment_status=fulfillment_status))
        elif after:
            orders = shopify.Order.find(status=status, created_at_min=after, limit=limit, fulfillment_status=fulfillment_status)
            if len(orders) > 1:
                orders.extend(getAllShopifyOrders(after=maxDate(orders), status=status, limit=limit, fulfillment_status=fulfillment_status))
        else:
            orders = shopify.Order.find(status=status, limit=limit, fulfillment_status=fulfillment_status)
            orders.extend(getAllShopifyOrders(before=minDate(orders), status=status, limit=limit, fulfillment_status=fulfillment_status))
            orders.extend(getAllShopifyOrders(after=maxDate(orders), status=status, limit=limit, fulfillment_status=fulfillment_status))
        return orders
    except (urllib.error.HTTPError, pyactiveresource.connection.ClientError):
        print("Waiting a moment to be polite. (And to not get cut off by the shopify api!)")
        sleep(2)
        return getAllShopifyOrders(before=before, after=after, status=status, limit=limit, fulfillment_status=fulfillment_status)
