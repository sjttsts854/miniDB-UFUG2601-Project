#include "miniDB.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
using namespace std;

void miniDB::CreateDataBase(const string& dbname) 
{
    string dir = "mkdir " + dbname;
    const char* d = dir.c_str();
    system(d);
    cout << "Database " << dbname << " created" << endl;
}

void miniDB::UseDataBase(const string& dbname) 
{
    string dir = "cd " + dbname;
    const char* d = dir.c_str();
    system(d);
    cout << "Using database " << dbname << endl;
}

void miniDB::CreateTable(const string& tableName, const vector<string>& columns) 
{
    tables[tableName] = Table(tableName, columns);
    cout << "Table " << tableName << " created" << endl;
}

void miniDB::DropTable(const string& tableName) 
{
    tables.erase(tableName);
    cout << "Table " << tableName << " dropped" << endl;
}

void miniDB::InsertIntoTable(const string& tableName, const vector<string>& row) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        tables[tableName].addRow(row);
        cout << "Row inserted into table " << tableName << endl;
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

void miniDB::SelectFromTable(const string& tableName) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        const Table& table = tables[tableName];
        for (const auto& col : table.columns) 
        {
            cout << col << " ";
        }
        cout << endl;
        for (const auto& row : table.data) 
        {
            for (const auto& cell : row) 
            {
                cout << cell << " ";
            }
            cout << endl;
        }
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

void miniDB::SaveTable(const string& tableName, const string& outputFile) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        tables[tableName].saveToFile(outputFile);
        cout << "Table " << tableName << " saved to " << outputFile << endl;
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

void miniDB::LoadTable(const string& tableName, const string& inputFile) 
{
    Table table(tableName, {});
    table.loadFromFile(inputFile);
    tables[tableName] = table;
    cout << "Table " << tableName << " loaded from " << inputFile << endl;
}

void miniDB::DeleteFromTable(const string& tableName, const string& condition) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        Table& table = tables[tableName];
        vector<vector<string>> newData;
        for (const auto& row : table.data) 
        {
            if (row[0] != condition) 
            {
                newData.push_back(row);
            }
        }
        table.data = newData;
        cout << "Rows deleted from table " << tableName << endl;
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

void miniDB::UpdateTable(const string& tableName, const string& condition, const string& set) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        Table& table = tables[tableName];
        for (auto& row : table.data) 
        {
            if (row[0] == condition) 
            {
                row[1] = set;
            }
        }
        cout << "Rows updated in table " << tableName << endl;
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

void parseCommand(const string& command, miniDB& db, const string& outputFile) 
{
    istringstream iss(command);
    vector<string> tokens;
    string token;
    while (iss >> token) 
    {
        tokens.push_back(token);
    }

    if (tokens.size() >= 3) 
    {
        if (tokens[0] == "CREATE" && tokens[1] == "DATABASE") 
        {
            db.CreateDataBase(tokens[2]);
        } 
        else if (tokens[0] == "USE" && tokens[1] == "DATABASE") 
        {
            db.UseDataBase(tokens[2]);
        } 
        else if (tokens[0] == "CREATE" && tokens[1] == "TABLE") 
        {
            vector<string> columns(tokens.begin() + 3, tokens.end());
            db.CreateTable(tokens[2], columns);
        } 
        else if (tokens[0] == "INSERT" && tokens[1] == "INTO") 
        {
            vector<string> row(tokens.begin() + 3, tokens.end());
            db.InsertIntoTable(tokens[2], row);
        } 
        else if (tokens[0] == "SELECT" && tokens[1] == "FROM") 
        {
            db.SelectFromTable(tokens[2]);
        } 
        else if (tokens[0] == "SAVE" && tokens[1] == "TABLE") 
        {
            db.SaveTable(tokens[2], outputFile);
        } 
        else if (tokens[0] == "LOAD" && tokens[1] == "TABLE") 
        {
            db.LoadTable(tokens[2], tokens[3]);
        }
        else 
        {
            cout << "Unknown command" << endl;
        }
    } 
    else 
    {
        cout << "Invalid command" << endl;
    }
}