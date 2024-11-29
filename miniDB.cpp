#include "miniDB.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <regex>
using namespace std;

bool FirstWrite = true;

string DBpath;

void parseOn(const string& conditions, string& col1, string& col2);

void parseWhere(const string& conditions, vector<pair<string, string>>& conditionPairs, string& logicalOperation, const vector<string>& columns) ;
// 从文件加载表
void miniDB::LoadAllTables(const string& path)
{
    DIR* dir;
    struct dirent* ent;
    if ((dir = opendir(path.c_str())) != NULL) 
    {
        // 遍历目录中的所有文件
        while ((ent = readdir(dir)) != NULL) 
        {
            string fileName = ent->d_name;
            // 检查文件扩展名是否为 .csv
            if (fileName.size() > 4 && fileName.substr(fileName.size() - 4) == ".csv") 
            {
                string tableName = fileName.substr(0, fileName.size() - 4);
                string filePath = path + "/" + fileName;
                ifstream file(filePath);
                if (!file.is_open()) 
                {
                    cerr << "Error: Could not open file: " << filePath << endl;
                    continue;
                }

                // 读取列名
                string line;
                getline(file, line);
                istringstream columnsStream(line);
                vector<string> columns;
                string col;
                while (getline(columnsStream, col, ',')) 
                {
                    columns.push_back(col);
                }

                // 创建表并加载数据
                vector<string> columnTypes(columns.size(), "TEXT"); // 默认所有列类型为 TEXT
                Table table(tableName, columns, columnTypes);

                // 读取数据行并确定列类型
                while (getline(file, line)) 
                {
                    istringstream rowStream(line);
                    vector<string> row;
                    string cell;
                    size_t colIndex = 0;
                    while (getline(rowStream, cell, ',')) 
                    {
                        row.push_back(cell);
                        // 确定列类型
                        if (columnTypes[colIndex] == "TEXT") 
                        {
                            try 
                            {
                                size_t pos;
                                int intValue = stoi(cell, &pos);
                                if (pos == cell.size()) 
                                {
                                    columnTypes[colIndex] = "INTEGER";
                                } 
                                else 
                                {
                                    float floatValue = stof(cell, &pos);
                                    if (pos == cell.size()) 
                                    {
                                        columnTypes[colIndex] = "FLOAT";
                                    }
                                }
                            } 
                            catch (const invalid_argument&) 
                            {
                                // 转换失败，保持为 TEXT
                            }
                        }
                        colIndex++;
                    }
                    table.addRow(row);
                }

                // 更新表的列类型
                table.columnTypes = columnTypes;
                tables[tableName] = table;
                file.close();
            }
        }
        closedir(dir);
    } 
    else 
    {
        cerr << "Error: Could not open directory: " << path << endl;
    }
}
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
    currentDB = dbname;
    DBpath = dbname ;
    LoadAllTables(DBpath);
    DBpath += "/";
}

// 创建表
void miniDB::CreateTable(const string& tableName, const vector<string>& columns, const vector<string>& columnTypes) 
{
    tables[tableName] = Table(tableName, columns, columnTypes);
    
    // 创建CSV文件
    ofstream file(DBpath+tableName + ".csv");
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
        ofstream file(DBpath+ tableName + ".csv", ios::app);
        if (!file.is_open()) 
        {
            cerr << "Error: Could not open file for writing: " << tableName << ".csv" << endl;
            return;
        }
        for (int i = 0; i < row.size(); ++i) 
        {
            if (tables[tableName].columnTypes[i] == "TEXT") 
            {
                file  << row[i] ;
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
    if (tables.find(tableName) != tables.end()) 
    {
        const Table& table = tables[tableName];
        vector<int> columnIndices;
        if (columns.empty()) 
        {
            // 如果没有指定列，则选择所有列
            for (size_t i = 0; i < table.columns.size(); ++i) 
            {
                columnIndices.push_back(i);
            }
        } 
        else 
        {
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
        }

        // 打开输出文件，使用追加模式
        ofstream file(outputFile, ios::out | ios::app);
        if (!file.is_open()) 
        {
            cout << "Failed to open file: " << outputFile << endl;
            return;
        }

        if (!FirstWrite) 
        {
            file << "---" << endl;
        }
        FirstWrite = false;
        // 写入列名
        for (size_t i = 0; i < columnIndices.size(); ++i) 
        {
            file << table.columns[columnIndices[i]];
            if (i < columnIndices.size() - 1) 
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
                    file<< row[colIndex] ;
                    cout << row[colIndex] << endl;
                } 
                else if (table.columnTypes[colIndex] == "FLOAT") 
                {
                    file << std::fixed << std::setprecision(2) << stof(row[colIndex]);
                    cout<<std::fixed << std::setprecision(2) << stof(row[colIndex])<<endl;
                } 
                else 
                {
                    file << row[colIndex];
                    cout << row[colIndex] << endl;
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


void miniDB::SelectFromTableWithConditions(const std::string& tableName, const std::vector<std::string>& columns, const std::string& conditions, const std::string& outputFile)
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
        string logicalOperation;

        parseWhere(conditions, conditionPairs, logicalOperation, columns);

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
            for (size_t i = 0; i < conditionPairs.size(); ++i) 
            {
                const auto& condPair = conditionPairs[i];
                auto it = find(table.columns.begin(), table.columns.end(), condPair.first);
                if (it != table.columns.end()) 
                {
                    int index = distance(table.columns.begin(), it);
                    string value = row[index];
                    string condValue;
                    if(condPair.second[0]!='!') condValue = condPair.second.substr(1);
                    else condValue = condPair.second.substr(2);
                    char op = condPair.second[0];
                    bool conditionMatch = false;

                    if (op == '>' && stof(value) > stof(condValue)) 
                    {
                        conditionMatch = true;
                    } 
                    else if (op == '<' && stof(value) < stof(condValue)) 
                    {
                        conditionMatch = true;
                    } 
                    else if (op == '=' && value == condValue) 
                    {
                        conditionMatch = true;
                    }
                    else if (op == '!' && value != condValue) 
                    {
                        conditionMatch = true;
                    }

                    if (i == 0) 
                    {
                        match = conditionMatch;
                    } 
                    else 
                    {
                        if (logicalOperation == "AND") 
                        {
                            match = match && conditionMatch;
                        } 
                        else if (logicalOperation == "OR") 
                        {
                            match = match || conditionMatch;
                        }
                    }
                }
            }

            if (match) 
            {
                for (size_t i = 0; i < columnIndices.size(); ++i) 
                {
                    int colIndex = columnIndices[i];
                    if (table.columnTypes[colIndex] == "TEXT") 
                    {
                        file<< row[colIndex] ;
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
        }

        file.close();
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

// 更新表中的数据

void miniDB::InnerJoin(const vector<string>& tableNames, const vector<string>& columns, const string& conditions, const string& outputFile)
{
    if (tables.find(tableNames[0]) != tables.end() && tables.find(tableNames[1]) != tables.end()) 
    {
        const Table& tableA = tables[tableNames[0]];
        const Table& tableB = tables[tableNames[1]];

        vector<int> columnIndicesA;
        vector<int> columnIndicesB;
        auto itA = find(tableA.columns.begin(), tableA.columns.end(), columns[0]);
        auto itB = find(tableB.columns.begin(), tableB.columns.end(), columns[1]);
        if (itA != tableA.columns.end() && itB != tableB.columns.end()) 
        {
            columnIndicesA.push_back(distance(tableA.columns.begin(), itA));
            columnIndicesB.push_back(distance(tableB.columns.begin(), itB));
        } 
        else 
        {
            cout << "Column " << columns[0] << " does not exist in table " << tableNames[0] << endl;
            cout << "Column " << columns[1] << " does not exist in table " << tableNames[1] << endl;
            return;
        }

        // 解析条件
        string colA,colB;
        parseOn(conditions, colA, colB);
        int indexA = distance(tableA.columns.begin(), find(tableA.columns.begin(), tableA.columns.end(), colA));
        int indexB = distance(tableB.columns.begin(), find(tableB.columns.begin(), tableB.columns.end(), colB));

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

        for (size_t i = 0; i < columns.size(); ++i) 
        {
            file << columns[i];
            if (i < columns.size() - 1) 
            {
                file << ",";
            }
        }
        file << endl;

        for (const auto& rowA : tableA.data) 
        {
            for (const auto& rowB : tableB.data) 
            {
                if (rowA[indexA] == rowB[indexB]) 
                {
                    if (tableA.columnTypes[columnIndicesA[0]] == "TEXT") 
                    {
                        file << rowA[columnIndicesA[0]];
                    } 
                    else if (tableA.columnTypes[columnIndicesA[0]] == "FLOAT") 
                    {
                        file << std::fixed << std::setprecision(2) << stof(rowA[columnIndicesA[0]]);
                    } 
                    else 
                    {
                        file << rowA[columnIndicesA[0]];
                    }
                    file << ",";
                    if (tableB.columnTypes[columnIndicesB[0]] == "TEXT") 
                    {
                        file << rowB[columnIndicesB[0]];
                    } 
                    else if (tableB.columnTypes[columnIndicesB[0]] == "FLOAT") 
                    {
                        file << std::fixed << std::setprecision(2) << stof(rowB[columnIndicesB[0]]);
                    } 
                    else 
                    {
                        file << rowB[columnIndicesB[0]];
                    }
                    file << endl;
                }
            }
        }

        file.close();
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
                valuesStr.erase(remove(valuesStr.begin(), valuesStr.end(), ')'), valuesStr.end());//this will work ?! why the underone don't work????
                valuesStr = removeSuffix(valuesStr, ";");// don't work  so strange???
                istringstream valuesStream(valuesStr);
                vector<string> values;
                string value;
                while (getline(valuesStream, value, ',')) 
                {
                    value.erase(0, value.find_first_not_of("; \t\n\r"));//this works ?!
                    value.erase(value.find_last_not_of("; \t\n\r") + 1);
                    values.push_back(value);
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
            size_t joinPos = find(tokens.begin(), tokens.end(), "INNER") - tokens.begin();
            if (joinPos != tokens.size())
            {
                string tableA = tokens[joinPos - 1];
                string tableB = tokens[joinPos + 2];
                vector<string> tableNames;
                tableNames.push_back(tableA);
                tableNames.push_back(tableB);
                vector<string> columns;
                for (size_t i = 1; i < fromPos; ++i) 
                {
                    tokens[i].erase(remove(tokens[i].begin(), tokens[i].end(), ','), tokens[i].end());
                    size_t dotPos = tokens[i].find(".");
                    if (dotPos != string::npos) 
                    {
                        columns.push_back(tokens[i].substr(dotPos + 1));
                    } 
                }
                string conditions;
                size_t onPos = find(tokens.begin(), tokens.end(), "ON") - tokens.begin();
                for (size_t i = onPos + 1; i < tokens.size(); ++i) 
                {
                    conditions += tokens[i];
                    conditions += " ";
                }
                db.InnerJoin(tableNames, columns, conditions, outputFile);
            }
            else if (fromPos != tokens.size()) 
            {
                string tableName = tokens[fromPos + 1];
                vector<string> columns;
                if (tokens[1] == "*") 
                {
                    columns = {}; // 空的 columns 表示选择所有列
                } 
                else 
                {
                    columns = vector<string>(tokens.begin() + 1, tokens.begin() + fromPos);
                    // 去掉列名中的逗号
                    for (auto& col : columns) 
                    {
                        col.erase(remove(col.begin(), col.end(), ','), col.end());
                    }
                }
                if (wherePos != tokens.size()) 
                {
                    string conditions;
                    for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                    {
                        conditions += tokens[i];
                        conditions += " ";
                    }
                    db.SelectFromTableWithConditions(tableName, columns, conditions, outputFile);
                } 
                else 
                {
                    db.SelectColumnsFromTable(tableName, columns, outputFile);
                }
            }
            else 
            {
                cout << "Invalid SELECT command" << endl;
            }
        }
        else if (tokens[0] == "DROP" && tokens[1] == "TABLE") 
        {
            db.DropTable(tokens[2]);
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

void parseWhere(const string& conditions, vector<pair<string, string>>& conditionPairs, string& logicalOperation, const vector<string>& columns) 
{
    vector<string> tokens;
    string token;
    istringstream iss(conditions);
    char ch;
    bool inQuotes = false;
    string currentToken;

    while (iss >> std::noskipws >> ch)
    {
        if (ch == '\'')
        {
            inQuotes = !inQuotes;
            currentToken += ch;
        }
        else if (inQuotes)
        {
            currentToken += ch;
        }
        else if (ch == ' ' && !inQuotes)
        {
            if (!currentToken.empty())
            {
                tokens.push_back(currentToken);
                currentToken.clear();
            }
        }
        else
        {
            currentToken += ch;
        }
    }

        // 处理最后一个 token
    if (!currentToken.empty())
    {
        tokens.push_back(currentToken);
    }

    auto it = find(tokens.begin(), tokens.end(), "AND");
    if (it != tokens.end()) 
    {
        logicalOperation = "AND";
        tokens.erase(it);
    } 
    else 
    {
        it = find(tokens.begin(), tokens.end(), "OR");
        if (it != tokens.end()) 
        {
            logicalOperation = "OR";
            tokens.erase(it);
        }
    }

    while (!tokens.empty())
    {
        string op = tokens[1];
        if(find(columns.begin(), columns.end(), tokens[0]) != columns.end())
        {
            string col = tokens[0];
            string value = tokens[2];
            conditionPairs.push_back({col, op + value});
        }
        else if(find(columns.begin(), columns.end(), tokens[2]) != columns.end())
        {
            string col = tokens[2];
            string value = tokens[0];
            if(op == ">")
            {
                op = "<";
            }
            else if(op == "<")
            {
                op = ">";
            }
            conditionPairs.push_back({col, op + value});
        }
        tokens.erase(tokens.begin(), tokens.begin() + 3);
    }

}

void parseOn(const string& conditions, string& col1, string& col2)
{
    vector<string> tokens;
    string token;
    istringstream iss(conditions);
    char ch;
    bool inQuotes = false;
    string currentToken;
    while (iss >> std::noskipws >> ch) 
    {
        if (ch == '\'') 
        {
            inQuotes = !inQuotes;
            currentToken += ch;
        } 
        else if (inQuotes) 
        {
            currentToken += ch;
        } 
        else if (ch == ' ' && !inQuotes) 
        {
            if (!currentToken.empty()) 
            {
                tokens.push_back(currentToken);
                currentToken.clear();
            }
        } 
        else 
        {
            currentToken += ch;
        }
    }
    size_t dotPosA = tokens[0].find(".");
    size_t dotPosB = tokens[2].find(".");
    if (dotPosA != string::npos && dotPosB != string::npos) 
    {
        string colA = tokens[0].substr(dotPosA + 1);
        string colB = tokens[2].substr(dotPosB + 1);
        col1 = colA;
        col2 = colB;
    }
}