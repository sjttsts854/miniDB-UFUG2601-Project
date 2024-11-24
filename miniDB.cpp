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
    cout << "Table " << tableName << " created and saved to " << tableName << ".csv" << endl;
}
//删除表
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
void miniDB::SelectFromTableWithConditions(const std::string& tableName, const std::string& columns, const std::string& conditions)
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
                size_t pos = cond.find_first_of("><=");
                if (pos != string::npos) 
                {
                    string column = cond.substr(0, pos);
                    string value = cond.substr(pos);
                    conditionPairs.push_back({column, value});
                }
            }
        }

        // 写入列名
        for (const auto& col : columns) {
            cout << col << " ";
        }
        cout << endl;

        // 写入数据行
        for (const auto& row : table.data) {
            bool match = true;
            for (const auto& condPair : conditionPairs) {
                if (condPair.first == "AND" || condPair.first == "OR") {
                    continue;
                }
                auto it = find(table.columns.begin(), table.columns.end(), condPair.first);
                if (it != table.columns.end()) {
                    int index = distance(table.columns.begin(), it);
                    string value = row[index];
                    string condValue = condPair.second.substr(1);
                    char op = condPair.second[0];
                    if (op == '>' && !(stof(value) > stof(condValue))) {
                        match = false;
                    } else if (op == '<' && !(stof(value) < stof(condValue))) {
                        match = false;
                    } else if (op == '=' && !(value == condValue)) {
                        match = false;
                    }
                }
            }
            if (match) {
                for (const auto& index : columnIndices) {
                    cout << row[index] << " ";
                }
                cout << endl;
            }
        }
    } else {
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
                    string columns = tokens[1];
                    string conditions;
                    for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                    {
                        conditions += tokens[i];
                        conditions += " ";
                    }
                    db.SelectFromTableWithConditions(tableName, columns, conditions);
                } 
                else if (tokens[1] == "*") 
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