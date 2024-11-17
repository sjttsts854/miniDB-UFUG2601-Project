#include "miniDB.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;

// 构造函数
miniDB::miniDB() {}

// 创建数据库
void miniDB::CreateDataBase(const string& dbname) 
{
    string dir = "mkdir " + dbname;
    const char* d = dir.c_str();
    system(d);
    cout << "Database " << dbname << " created" << endl;
}

// 使用数据库
void miniDB::UseDataBase(const string& dbname) 
{
    string dir = "cd " + dbname;
    const char* d = dir.c_str();
    system(d);
    cout << "Using database " << dbname << endl;
}

// 创建表
void miniDB::CreateTable(const string& tableName, const vector<string>& columns, const vector<string>& columnTypes) 
{
    tables[tableName] = Table(tableName, columns, columnTypes);
    cout << "Table " << tableName << " created" << endl;
}

// 删除表
void miniDB::DropTable(const string& tableName) 
{
    tables.erase(tableName);
    cout << "Table " << tableName << " dropped" << endl;
}

// 向表中插入数据
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

// 从表中选择数据
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

// 从表中选择特定列的数据
void miniDB::SelectColumnsFromTable(const string& tableName, const vector<string>& columns) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        const Table& table = tables[tableName];
        vector<int> columnIndices;
        for (const auto& col : columns) 
        {
            auto it = find(table.columns.begin(), table.columns.end(), col);
            if (it != table.columns.end()) 
            {
                columnIndices.push_back(distance(table.columns.begin(), it));
            } 
            else 
            {
                cout << "Column " << col << " does not exist in table " << tableName << endl;
                return;
            }
        }
        for (const auto& col : columns) 
        {
            cout << col << " ";
        }
        cout << endl;
        for (const auto& row : table.data) 
        {
            for (const auto& index : columnIndices) 
            {
                cout << row[index] << " ";
            }
            cout << endl;
        }
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

// 从表中选择所有列的数据
void miniDB::SelectAllColumnsFromTable(const string& tableName) 
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

// 从表中选择带有条件的数据
void miniDB::SelectFromTableWithCondition(const std::string& tableName, const std::string& column, const std::string& condition)
{
    if(tables.find(tableName) != tables.end())
    {
        const Table& table = tables[tableName];
        auto it = find(table.columns.begin(), table.columns.end(), column);
        if(it != table.columns.end())
        {
            int columnIndex = distance(table.columns.begin(), it);
            for(const auto& col : table.columns)
            {
                cout << col << " ";
            }
            cout << endl;
            for(const auto& row : table.data)
            {
                if(row[columnIndex] == condition)
                {
                    for(const auto& cell : row)
                    {
                        cout << cell << " ";
                    }
                    cout << endl;
                }
            }
        }
        else
        {
            cout << "Column " << column << " does not exist in table " << tableName << endl;
        }
    }
    else
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}
// 将表数据保存到文件
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

// 从文件加载表数据
void miniDB::LoadTable(const string& tableName, const string& inputFile) 
{
    Table table(tableName, {}, {});
    table.loadFromFile(inputFile);
    tables[tableName] = table;
    cout << "Table " << tableName << " loaded from " << inputFile << endl;
}

// 将表数据保存为 CSV 文件
void miniDB::SaveTableToCSV(const string& tableName, const string& outputFile) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        tables[tableName].saveToCSV(outputFile);
        cout << "Table " << tableName << " saved to CSV file " << outputFile << endl;
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

// 将查询结果保存为 CSV 文件
void miniDB::SelectColumnsFromTableToCSV(const string& tableName, const vector<string>& columns, const string& outputFile) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        const Table& table = tables[tableName];
        vector<int> columnIndices;
        for (const auto& col : columns) 
        {
            auto it = find(table.columns.begin(), table.columns.end(), col);
            if (it != table.columns.end()) 
            {
                columnIndices.push_back(distance(table.columns.begin(), it));
            } 
            else 
            {
                cout << "Column " << col << " does not exist in table " << tableName << endl;
                return;
            }
        }

        ofstream file(outputFile);
        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file for writing: " << outputFile << endl;
            return;
        }

        // 写入列名
        for (size_t i = 0; i < columns.size(); ++i) 
        {
            file << columns[i];
            if (i < columns.size() - 1) 
            {
                file << ",";
            }
        }
        file << endl;

        // 写入数据行
        for (const auto& row : table.data) {
            for (size_t i = 0; i < columnIndices.size(); ++i) 
            {
                int index = columnIndices[i];
                if (table.columnTypes[index] == "TEXT") 
                {
                    file << "\"" << row[index] << "\"";
                } 
                else if (table.columnTypes[index] == "FLOAT") 
                {
                    file << fixed << setprecision(2) << stof(row[index]);
                } 
                else 
                {
                    file << row[index];
                }
                if (i < columnIndices.size() - 1) 
                {
                    file << ",";
                }
            }
            file << endl;
        }

        file.close();
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

// 解析并执行命令
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
            string tableName = tokens[2];
            vector<string> columns;
            vector<string> columnTypes;
            for (size_t i = 3; i < tokens.size(); i += 2) 
            {
                columns.push_back(tokens[i]);
                columnTypes.push_back(tokens[i + 1]);
            }
            db.CreateTable(tableName, columns, columnTypes);
        } 
        else if (tokens[0] == "INSERT" && tokens[1] == "INTO") 
        {
            string tableName = tokens[2];
            size_t pos = command.find("VALUES");
            if (pos != string::npos) 
            {
                string valuesStr = command.substr(pos + 6); // 跳过 "VALUES"
                valuesStr.erase(remove(valuesStr.begin(), valuesStr.end(), '('), valuesStr.end());
                valuesStr.erase(remove(valuesStr.begin(), valuesStr.end(), ')'), valuesStr.end());
                istringstream valuesStream(valuesStr);
                vector<string> values;
                string value;
                while (valuesStream >> value) 
                {
                    values.push_back(value);
                    if (valuesStream.peek() == ',') 
                    {
                        valuesStream.ignore();
                    }
                }
                db.InsertIntoTable(tableName, values);
            } 
            else 
            {
                cout << "Invalid INSERT INTO command" << endl;
            }
        } 
        else if (tokens[0] == "SELECT")
        {
            size_t fromPos = find(tokens.begin(), tokens.end(), "FROM") - tokens.begin();
            if (fromPos != tokens.size()) 
            {
                string tableName = tokens[fromPos + 1];
                if (tokens[1] == "*") 
                {
                    db.SelectAllColumnsFromTable(tableName);
                } 
                else 
                {
                    vector<string> columns(tokens.begin() + 1, tokens.begin() + fromPos);
                    db.SelectColumnsFromTableToCSV(tableName, columns, outputFile);
                }
            }
            else 
            {
                cout << "Invalid SELECT command" << endl;
            }
        } 
        else if (tokens[0] == "SAVE" && tokens[1] == "TABLE") 
        {
            db.SaveTable(tokens[2], outputFile);
        } 
        else if (tokens[0] == "LOAD" && tokens[1] == "TABLE") 
        {
            db.LoadTable(tokens[2], tokens[3]);
        } 
        else if (tokens[0] == "SAVE" && tokens[1] == "TABLETOCSV") 
        {
            db.SaveTableToCSV(tokens[2], outputFile);
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