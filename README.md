# csv_handle
将当前目录下的所有txt转换成csv.然后导入clickhouse数据库,并且生成arrow ipc文件

## first
在数据库中创建一张表,名为taxonomy.
```
create table taxonomy
(
    abbreviation String,
    name         String,
    genomes Array(String)
)
    engine = MergeTree PRIMARY KEY abbreviation
        ORDER BY abbreviation
        SETTINGS index_granularity = 8192;
```
在程序所在目录创建文件夹'arrow'

## last
执行python脚本,给所有物种增加学名.
```
import json
from clickhouse_driver import Client

client = Client(host='localhost')
with open('ABB.json') as j:
    data: dict = json.load(j)
for k, v in data.items():
    sql = f"ALTER TABLE taxonomy UPDATE name = '{v}' WHERE abbreviation = '{k}'"
    client.execute(sql)
```
ABB.json
```
{
  "ath": "Arabidopsis thaliana",
  "bsu": "Bacillus subtilis",
  "cel": "Caenorhabditis elegans",
  "ctr": "Escherichia coli",
  "hel": "Helicobacter pylori",
  "hpy": "Haemophilus pylori",
  "mtu": "Mycobacterium tuberculosis",
  "rpr": "Drosophila melanogaster",
  "sce": "Saccharomyces cerevisiae",
  "son": "Solanum lycopersicum",
  "sty": "Salmonella Typhi",
  "vch": "Vibrio cholerae",
  "xcc": "Xanthomonas campestris",
  "bme": "Bacillus megaterium",
  "cac": "Candida albicans",
  "cje": "Campylobacter jejuni",
  "eco": "Escherichia coli",
  "hin": "Helicobacter influenzae",
  "hsa": "Homo sapiens",
  "pae": "Pseudomonas aeruginosa",
  "sau": "Staphylococcus aureus",
  "sco": "Streptomyces coelicolor",
  "spy": "Streptococcus pyogenes",
  "syn": "Synechocystis sp.",
  "wol": "Wolbachia"
}
```