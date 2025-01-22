from db_connection import DBConnection as dbc
from dbobject import DBSelectObject

class CheckFigureRelation:

    def First():
        p2 = DBSelectObject("Pifagor2UkDev", "tab", "FigureRelation")
        dwh = DBSelectObject("DWH", "stg_p2_ukdev_tab", "FigureRelation")

        dfp2 = p2.get_df()
        
        #lp2 = list(p2)
        #print(len(lp2))

        #ldwh = list(dwh)
        #print(len(ldwh))
        

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
