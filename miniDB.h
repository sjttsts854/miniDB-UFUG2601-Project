#ifndef MINIDB_H
#define MINIDB_H

#include "Table.h"
#include <map>
#include <string>

class miniDB
{
public:
    std::map<std::string, Table> tables;

    miniDB(){};

    void CreateDataBase(const std::string& dbname);
    void UseDataBase(const std::string& dbname);
    void CreateTable(const std::string& tableName, const std::vector<std::string>& columns);
    void DropTable(const std::string& tableName);
    void InsertIntoTable(const std::string& tableName, const std::vector<std::string>& row);
    void SelectFromTable(const std::string& tableName);
    void SaveTable(const std::string& tableName, const std::string& outputFile);
    void LoadTable(const std::string& tableName, const std::string& inputFile);
    void DeleteFromTable(const std::string& tableName, const std::string& condition);
    void UpdateTable(const std::string& tableName, const std::string& condition, const std::string& set);
};

void parseCommand(const std::string& command, miniDB& db, const std::string& outputFile);



#endif