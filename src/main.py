from Database.DatabaseManager import DatabaseManager

from utils.Database.databaseConfig import devices_list


def main():
    dbManager = DatabaseManager()
    conn = dbManager.connect_database()
    tableExist = dbManager.checkTable_existence("plant_metrics")
    print(tableExist)

    #check_table(db_conn, "plants_metrics")



if __name__ == "__main__":
    main()
