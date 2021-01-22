import os
from tqdm import tqdm
for gb in tqdm(os.listdir("./../../genbank")):
    if ".gb" in gb:
        os.system('/opt/gslbiotech/snapgene-server/snapgene-server.sh --command \'{"request": "generatePNGMap", "inputFile": "/home/ubuntu/freegenes/genbank/'+gb+'", "outputPng": "/home/ubuntu/freegenes/genes/images/'+gb.replace(".gb", ".png")+'"}\' > ~/snapgene.log 2>&1')
os.system("rm -rf tmp_files")
