#include "miniDB.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;



// 创建数据库
void miniDB::CreateDataBase(const string& dbname) 
{
    string dir = "mkdir " + dbname;
    const char* d = dir.c_str();
    system(d);
}

// 使用数据库
void miniDB::UseDataBase(const string& dbname) 
{
    string dir = "cd " + dbname;
    const char* d = dir.c_str();
    system(d);
}

// 创建表
void miniDB::CreateTable(const string& tableName, const vector<string>& columns, const vector<string>& columnTypes) 
{
    tables[tableName] = Table(tableName, columns, columnTypes);
    
    // 创建CSV文件
    ofstream file(tableName + ".csv");
    if (!file.is_open()) 
    {
        cerr << "Error: Could not create file: " << tableName << ".csv" << endl;
        return;
    }

    // 写入列名和列类型
    for (int i = 0; i < columns.size(); ++i) 
    {
        file << columns[i];
        if (i < columns.size() - 1) 
        {
            file << ",";
        }
    }
    file << endl;

    file.close();
}

//删除表
void miniDB::DropTable(const string& tableName) 
{
    tables.erase(tableName);
    string file = tableName + ".csv";
    remove(file.c_str());
}

// 向表中插入数据
void miniDB::InsertIntoTable(const string& tableName, const vector<string>& row) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        tables[tableName].addRow(row);
        ofstream file(tableName + ".csv", ios::app);
        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file for writing: " << tableName << ".csv" << endl;
            return;
        }
        for (int i = 0; i < row.size(); ++i) 
        {
            if (tables[tableName].columnTypes[i] == "TEXT") 
            {
                file << "'" << row[i] << "'";
            }
            else if(tables[tableName].columnTypes[i] == "FLOAT")
            {
                file << fixed << setprecision(2) << stof(row[i]);
            }
            else
            {
                file << row[i];
            }

            if (i < row.size() - 1) 
            {
                file << ",";
            }
        }
        file << endl;
        file.close();
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

// 从表中选择特定列的数据
void miniDB::SelectColumnsFromTable(const string& tableName, const vector<string>& columns, const string& outputFile) 
{
    static bool FirstWrite = true;
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

        // 打开输出文件
        ofstream file(outputFile, ios::out | ios::app);

        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file: " << outputFile << endl;
            return;
        }

        if (!FirstWrite) 
        {
            file << "---" << endl;
        }
        FirstWrite = false;

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
        for (const auto& row : table.data) 
        {
            for (size_t i = 0; i < columnIndices.size(); ++i) 
            {
                int colIndex = columnIndices[i];
                if (table.columnTypes[colIndex] == "TEXT") 
                {
                    file << "'" << row[colIndex] << "'";
                } 
                else if (table.columnTypes[colIndex] == "FLOAT") 
                {
                    file << std::fixed << std::setprecision(2) << stof(row[colIndex]);
                } 
                else 
                {
                    file << row[colIndex];
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

// 从表中选择所有列的数据
void miniDB::SelectAllColumnsFromTable(const string& tableName, const string& outputFile) 
{
    static bool FirstWrite = true;
    if (tables.find(tableName) != tables.end()) 
    {
        const Table& table = tables[tableName];

        // 打开输出文件
        ofstream file(outputFile, ios::out | ios::app);

        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file: " << outputFile << endl;
            return;
        }

        if (!FirstWrite) 
        {
            file << "---" << endl;
        }
        FirstWrite = false;
        
        // 写入列名
        for (size_t i = 0; i < table.columns.size(); ++i) 
        {
            file << table.columns[i];
            if (i < table.columns.size() - 1) 
            {
                file << ",";
            }
        }
        file << endl;

        // 写入数据行
        for (const auto& row : table.data) 
        {
            for (size_t i = 0; i < row.size(); ++i) 
            {
                if (table.columnTypes[i] == "TEXT") 
                {
                    file << "'" << row[i] << "'";
                } 
                else if (table.columnTypes[i] == "FLOAT") 
                {
                    file << std::fixed << std::setprecision(2) << stof(row[i]);
                } 
                else 
                {
                    file << row[i];
                }
                if (i < row.size() - 1) 
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

// 从表中选择带有条件的数据
void miniDB::SelectFromTableWithConditions(const std::string& tableName, const std::vector<std::string>& columns, const std::string& conditions, const std::string& outputFile)
{
    static bool FirstWrite = true;
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

        // 解析条件
        vector<pair<string, string>> conditionPairs;
        istringstream condStream(conditions);
        string cond;
        while (getline(condStream, cond, ' ')) 
        {
            if (cond == "AND" || cond == "OR") 
            {
                conditionPairs.push_back({cond, ""});
            } 
            else 
            {
                size_t opPos = cond.find_first_of("><=");
                if (opPos != string::npos) 
                {
                    string colName = cond.substr(0, opPos);
                    string op = cond.substr(opPos, 1);
                    string value = cond.substr(opPos + 1);
                    conditionPairs.push_back({colName, op + value});
                }
            }
        }

        // 打开输出文件
        ofstream file(outputFile, ios::out | ios::app);

        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file: " << outputFile << endl;
            return;
        }

        if (!FirstWrite) 
        {
            file << "---" << endl;
        }
        FirstWrite = false;

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
        for (const auto& row : table.data) 
        {
            bool match = true;
            for (const auto& condPair : conditionPairs) 
            {
                auto it = find(table.columns.begin(), table.columns.end(), condPair.first);
                if (it != table.columns.end()) 
                {
                    int index = distance(table.columns.begin(), it);
                    string value = row[index];
                    string condValue = condPair.second.substr(1);
                    char op = condPair.second[0];
                    if (op == '>' && !(stof(value) > stof(condValue))) 
                    {
                        match = false;
                    } 
                    else if (op == '<' && !(stof(value) < stof(condValue))) 
                    {
                        match = false;
                    } 
                    else if (op == '=' && !(value == condValue)) 
                    {
                        match = false;
                    }
                }
            }
            if (match) 
            {
                for (size_t i = 0; i < columnIndices.size(); ++i) 
                {
                    if (table.columnTypes[columnIndices[i]] == "TEXT") 
                    {
                        file << "'" << row[columnIndices[i]] << "'";
                    } 
                    else if (table.columnTypes[columnIndices[i]] == "FLOAT") 
                    {
                        file << std::fixed << std::setprecision(2) << stof(row[columnIndices[i]]);
                    } 
                    else 
                    {
                        file << row[columnIndices[i]];
                    }
                    if (i < columnIndices.size() - 1) 
                    {
                        file << ",";
                    }
                }
                file << endl;
            }
        }

        file.close();
        
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}




string removeSuffix(const string& str, const string& suffix) 
{
    if (str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0) 
    {
        return str.substr(0, str.size() - suffix.size());
    }
    return str;
}

// 解析并执行命令
void parseCommand(const string& command, miniDB& db, const string& outputFile) 
{
    istringstream iss(command);
    vector<string> tokens;
    string token;
    while (iss >> token) 
    {
        token = removeSuffix(token, ";");
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
            size_t start = command.find("(");
            size_t end = command.find(")");
            if (start != string::npos && end != string::npos) 
            {
                string columnsStr = command.substr(start + 1, end - start - 1);
                istringstream columnsStream(columnsStr);
                string col;
                while (getline(columnsStream, col, ',')) 
                {
                    istringstream colStream(col);
                    string colName, colType;
                    colStream >> colName >> colType;
                    columns.push_back(colName);
                    columnTypes.push_back(colType);
                }
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
            size_t wherePos = find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
            if (fromPos != tokens.size()) 
            {
                string tableName = tokens[fromPos + 1];
                if (wherePos != tokens.size()) 
                {
                    vector<string> columns(tokens.begin() + 1, tokens.begin() + fromPos);
                    // 去掉列名中的逗号
                    for (auto& col : columns) 
                    {
                        col.erase(remove(col.begin(), col.end(), ','), col.end());
                    }
                    string conditions;
                    for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                    {
                        conditions += tokens[i];
                        conditions += " ";
                    }
                    db.SelectFromTableWithConditions(tableName, columns, conditions, outputFile);
                } 
                else if (tokens[1] == "*") 
                {
                    db.SelectAllColumnsFromTable(tableName, outputFile);
                } 
                else 
                {
                    vector<string> columns(tokens.begin() + 1, tokens.begin() + fromPos);
                    // 去掉列名中的逗号
                    for (auto& col : columns) 
                    {
                        col.erase(remove(col.begin(), col.end(), ','), col.end());
                    }
                    db.SelectColumnsFromTable(tableName, columns, outputFile);
                }
            }
            else 
            {
                cout << "Invalid SELECT command" << endl;
            }
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