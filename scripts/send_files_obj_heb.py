import argparse
import subprocess
ap = argparse.ArgumentParser()
ap.add_argument("-d", "--semaine", required = True, help="week")
args = vars(ap.parse_args())


print("************** Read from Elasticsearch **************")




print("************** Loading files **************")

df_grossiste = pd.read_csv("/nfs_sunshine/random_objectif_hebdomadaire_grossiste_202210.csv", sep = ";")
df_grossiste.head()

df_agent = pd.read_csv("/nfs_sunshine/random_objectif_hebdomadaire_agent_202210.csv", sep = ";")
df_agent.head()

print("************** Saving files **************")

df_grossiste[["COMMISSION_GROSSISTE_FIXE","COMMISSION_GROSSISTE_REEL","LAST_DAY_OF_WEEK","OBJECTIF_REALISE_GROSSISTE",
    "OBJECTIF_SEMAINE","PRIME_HEBDOMADAIRE_GROSSISTE", "R/O_GROSSISTE", "SEMAINE", "TETE_DE_PONT", 
    "TRANSACTION_TAG"]][df_grossiste["SEMAINE"] == int(args["semaine"])].to_csv(f"/nfs_sunshine/random_objectif_hebdomadaire_grossiste_202210_{str(args["semaine"])}.csv", sep = ";", index = False)

df_agent[["COMMISSION_AGENT_FIXE","COMMISSION_AGENT_REEL","COMPTE_PDV","COMPTE_REVENDEUR",
    "LAST_DAY_OF_WEEK","OBJECTIF_REALISE_AGENT", "OBJECTIF_SEMAINE", "PRIME_HEBDOMADAIRE_AGENT", "R/O_AGENT", 
    "SEMAINE", "TETE_DE_PONT", "TRANSACTION_TAG"]][df_agent["SEMAINE"] == int(args["semaine"])].to_csv(f"/nfs_sunshine/random_objectif_hebdomadaire_agent_202210_{str(args["semaine"])}.csv", sep = ";", index = False)


