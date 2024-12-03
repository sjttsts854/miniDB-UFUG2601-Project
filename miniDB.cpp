#include "miniDB.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unordered_map>
#include <stack>

using namespace std;

bool FirstWrite = true;

string DBpath;

bool parseCondition(const Table& table,const vector<pair<string, string>>& conditionPairs, string logicalOperation, const vector<string>& row);

bool isOperator(char c) 
{
    return c == '+' || c == '-' || c == '*' || c == '/';
}

vector<string> splitExpression(const string& expr) 
{
    vector <string> tokens;
    string token;
    for (size_t i=0; i<expr.size();++i)
    {
        char current = expr[i];
        if (current == ' ')
        {
            continue;
        }
        if (isdigit(current) || current == '.') {
            // 处理数字
            token.clear();
            while (i < expr.size() && (isdigit(expr[i]) || expr[i] == '.')) {
                token += expr[i];
                ++i;
            }
            tokens.push_back(token);
            --i; // 恢复索引
        }
        else if (isalpha(current)) {
            // 处理变量（列名）
            token.clear();
            while (i < expr.size() && (isalnum(expr[i]) || expr[i] == '_')) {
                token += expr[i];
                ++i;
            }
            tokens.push_back(token);
            --i;
        }
        else if (isOperator(current) || current == '(' || current == ')') {
            // 处理操作符和括号
            token = current;
            tokens.push_back(token);
        }
        else {
            // 处理其他字符（可根据需要扩展）
            token = current;
            tokens.push_back(token);
        }
    }
    return tokens;
}

int precedence(char op) 
{
    if (op == '+' || op == '-') 
    {
        return 1;
    }
    if (op == '*' || op == '/') 
    {
        return 2;
    }
    return 0;
}

// 将中缀表达式转换为后缀表达式
vector<string> infixToPostfix(const string& expr) 
{
    vector<string> output;
    stack<char> opStack;
    vector<string> tokens = splitExpression(expr);
    
    for(const auto& token : tokens)
    {
        if(isdigit(token[0]) || (token[0] == '.' && token.size() > 1))
        {
            // 数字
            output.push_back(token);
        }
        else if(isalpha(token[0]) || std::isdigit(token[0]))
        {
            // 变量（列名），在替换后应为数字
            output.push_back(token);
        }
        else if(token == "(")
        {
            opStack.push('(');
        }
        else if(token == ")")
        {
            while(!opStack.empty() && opStack.top() != '(')
            {
                output.push_back(std::string(1, opStack.top()));
                opStack.pop();
            }
            if(!opStack.empty()) opStack.pop(); // 弹出 '('
        }
        else if(isOperator(token[0]))
        {
            while(!opStack.empty() && precedence(opStack.top()) >= precedence(token[0]))
            {
                output.push_back(string(1, opStack.top()));
                opStack.pop();
            }
            opStack.push(token[0]);
        }
    }
    while(!opStack.empty())
    {
        output.push_back(string(1, opStack.top()));
        opStack.pop();
    }
    return output;
}

double evaluatePostfix(const vector<string>& postfix, const unordered_map<string, double>& variables) 
{
    stack<double> valStack;
    for (const string& token : postfix) 
    {
        if (isdigit(token[0]) || (token[0] == '.' && token.size() > 1)) 
        {
            valStack.push(stod(token));
        } 
        else if (isalpha(token[0])) 
        {
            if (variables.count(token)) 
            {
                valStack.push(variables.at(token));
            } 
            else 
            {
                throw runtime_error("Undefined variable: " + token);
            }
        } 
        else if (isOperator(token[0])) 
        {
            double b = valStack.top(); valStack.pop();
            double a = valStack.top(); valStack.pop();
            switch (token[0]) 
            {
                case '+': valStack.push(a + b); break;
                case '-': valStack.push(a - b); break;
                case '*': valStack.push(a * b); break;
                case '/': valStack.push(a / b); break;
            }
        }
    }
    return valStack.top();
}

void parseSet(const string& setClauses, vector<pair<string, string>>& setPairs, const vector<string>& columns, const vector<string>& columnTypes) ;

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

// 从表中选择带有条件的数据
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

            if (parseCondition(table, conditionPairs, logicalOperation, row)) 
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

// INNER JOIN 两个表
void miniDB::InnerJoin(const vector<string>& tableNames, const vector<string>& columns, const string& conditions, const string& Whereconditions, const string& outputFile)
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
        vector<pair<string, string>> conditionPairs;
        string logicalOperation;
        parseWhere(Whereconditions, conditionPairs, logicalOperation, tableA.columns);
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
            file << tableNames[i] <<"."<<columns[i];
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
                    if(!conditions.empty())
                    {
                        if(!parseCondition(tableA, conditionPairs, logicalOperation, rowA))continue;
                    }
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

//删除表中的行
void miniDB::Delete(const string& tableName, const string& conditions) 
{
    if (tables.find(tableName) != tables.end()) 
    {
        Table& table = tables[tableName];
        vector<pair<string, string>> conditionPairs;
        string logicalOperation;

        if (!conditions.empty()) 
        {
            parseWhere(conditions, conditionPairs, logicalOperation, table.columns);
        }

        auto it = table.data.begin();
        while (it != table.data.end()) 
        {
            bool match = true;
            if (!conditions.empty()) 
            {
                for (const auto& condPair : conditionPairs) 
                {
                    auto colIt = find(table.columns.begin(), table.columns.end(), condPair.first);
                    if (colIt != table.columns.end()) 
                    {
                        int index = distance(table.columns.begin(), colIt);
                        string value = (*it)[index];
                        string condValue = condPair.second.substr(1);
                        char op = condPair.second[0];

                        if (op == '>' && stof(value) <= stof(condValue)) 
                        {
                            match = false;
                            break;
                        } 
                        else if (op == '<' && stof(value) >= stof(condValue)) 
                        {
                            match = false;
                            break;
                        } 
                        else if (op == '=' && value != condValue) 
                        {
                            match = false;
                            break;
                        } 
                        else if (op == '!' && value == condValue) 
                        {
                            match = false;
                            break;
                        }
                    }
                }
            }

            if (match) 
            {
                it = table.data.erase(it);
            } 
            else 
            {
                ++it;
            }
        }

        // 更新CSV文件
        table.saveToCSV(DBpath + tableName + ".csv");
    } 
    else 
    {
        cout << "Table " << tableName << " does not exist" << endl;
    }
}

//更新数据表
void miniDB::UpdateTable(const string& tableName, const string setClauses, const string& conditions)
{
    if (tables.find(tableName)!=tables.end())
    {
        Table& table = tables[tableName];
        vector <pair<string, string>> setPairs;
        vector <pair<string, string>> conditionPairs;
        string logicalOperation;
        parseSet(setClauses, setPairs, table.columns, table.columnTypes);

        if (!conditions.empty()) 
        {
            parseWhere(conditions, conditionPairs, logicalOperation, table.columns);
        }

        for (auto& row : table.data)
        {
            unordered_map<string, double> variables;
            for (size_t i=0; i<table.columns.size(); ++i)
            {
                if(table.columnTypes[i]!="TEXT")
                {
                    variables[table.columns[i]] = stod(row[i]);
                }
            }
            
            if(parseCondition(table, conditionPairs, logicalOperation, row))
            {
                for (const auto& setPair : setPairs)
                {
                    const string& col = setPair.first;
                    const string& expr = setPair.second;
                    size_t colIndex = distance(table.columns.begin(), find(table.columns.begin(), table.columns.end(), col));
                    if(table.columnTypes[colIndex]=="TEXT")
                    {
                        row[colIndex]=expr;
                    }
                    else
                    {
                        vector<string> tokens = splitExpression(expr);
                        for (auto& token : tokens) 
                        {
                            if (isalpha(token[0])) 
                            {
                                if(variables.find(token)!=variables.end())
                                    token = to_string(variables[token]);
                            }
                        }
                        string newExpr;
                        for(const auto& token : tokens)
                        {
                            newExpr += token;
                            newExpr += " ";
                        }
                        if(!newExpr.empty()&&newExpr.back()==' ')
                        {
                            newExpr.pop_back();
                        }
                        vector<string> postfix=infixToPostfix(newExpr);
                        double newValue = evaluatePostfix(postfix, variables);
                        row[colIndex]=to_string(newValue);
                        variables[col]=newValue;
                    }
                }
            }
        }
        table.saveToCSV(DBpath + tableName + ".csv");
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
                for (size_t i = onPos + 1; i < wherePos; ++i) 
                {
                    conditions += tokens[i];
                    conditions += " ";
                }
                string Whereconditions;
                for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                {
                    size_t dotPos = tokens[i].find(".");
                    if(dotPos != string::npos)
                    {
                        tokens[i] = tokens[i].substr(dotPos + 1);
                    }
                    Whereconditions += tokens[i];
                    Whereconditions += " ";
                }
                db.InnerJoin(tableNames, columns, conditions, Whereconditions, outputFile);
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
        else if (tokens[0] == "DELETE" && tokens[1] == "FROM") 
        {
            string tableName = tokens[2];
            string conditions;
            size_t wherePos = find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
            if(wherePos != tokens.size())
            {
                for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                {
                    conditions += tokens[i];
                    conditions += " ";
                }
                db.Delete(tableName, conditions);
            }
        }
        else if (tokens[0] == "UPDATE")
        {
            string tableName = tokens[1];
            string setClauses;
            string conditions;
            size_t setPos = find(tokens.begin(), tokens.end(), "SET") - tokens.begin();
            size_t wherePos = find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
            if(setPos != tokens.size())
            {
                for (size_t i = setPos + 1; i < wherePos; ++i) 
                {
                    setClauses += tokens[i];
                    setClauses += " ";
                }
                if (wherePos != tokens.size()) 
                {
                    for (size_t i = wherePos + 1; i < tokens.size(); ++i) 
                    {
                        size_t dotPos = tokens[i].find(".");
                        if(dotPos != string::npos)
                        {
                            tokens[i] = tokens[i].substr(dotPos + 1);
                        }
                        conditions += tokens[i];
                        conditions += " ";
                    }
                    db.UpdateTable(tableName, setClauses, conditions);
                }
                else
                {
                    db.UpdateTable(tableName, setClauses, "");
                }
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

void parseSet(const string& setClauses, vector<pair<string, string>>& setPairs, const vector<string>& columns, const vector<string>& columnTypes) 
{
    vector<string> clauses;
    string clause;
    istringstream iss(setClauses);
    while (getline(iss, clause, ',')) 
    {
        clauses.push_back(clause);
    }
    for (const auto& clause : clauses) 
    {
        size_t eqPos = clause.find("=");
        string colName;
        if (eqPos != string::npos) 
        {
            colName = clause.substr(0, eqPos);
            colName.erase(remove(colName.begin(), colName.end(), ' '), colName.end()); // 去除空格
        }
        string rhs;
        if (eqPos != string::npos) 
        {
            rhs = clause.substr(eqPos + 1);
        }

        // 获取列的数据类型
        auto it = find(columns.begin(), columns.end(), colName);
        if (it != columns.end())
        {
            setPairs.push_back({colName, rhs});
        }
        else
        {
            cerr << "Column " << colName << " does not exist." << endl;
        }
    }
}

bool parseCondition(const Table& table,const vector<pair<string, string>>& conditionPairs, string logicalOperation, const vector<string>& row)
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
    return match;
}