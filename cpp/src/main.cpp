#include <iostream>
#include <clickhouse/client.h>

using clickhouse::Block;
using clickhouse::Client;
using clickhouse::ClientOptions;
using clickhouse::ColumnString;
using clickhouse::ColumnUInt64;
using std::cout;
using std::endl;

int main()
{
    cout << "--------------------------------------------------------------------------------" << endl;
    cout << "CLIENT #1: SELECT id, name FROM test.numbers LIMIT 10" << endl;
    cout << "--------------------------------------------------------------------------------" << endl;
    Client client1(ClientOptions().SetHost("localhost").SetPort(9300));
    client1.Select(
        "SELECT id, name FROM test.numbers  LIMIT 10",
        [](const Block & block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i)
            {
                cout << block[0]->As<ColumnUInt64>()->At(i) << " " << block[1]->As<ColumnString>()->At(i) << endl;
            }
        });
    cout << "--------------------------------------------------------------------------------" << endl;

    cout << "--------------------------------------------------------------------------------" << endl;
    cout << "CLIENT #2: SELECT id, name FROM test.numbers LIMIT 10" << endl;
    cout << "--------------------------------------------------------------------------------" << endl;
    Client client2(ClientOptions().SetHost("localhost").SetPort(9300));
    client2.Select(
        "SELECT id, name FROM test.numbers LIMIT 10",
        [](const Block & block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i)
            {
                cout << block[0]->As<ColumnUInt64>()->At(i) << " " << block[1]->As<ColumnString>()->At(i) << endl;
            }
        });
    cout << "--------------------------------------------------------------------------------" << endl;


    cout << "--------------------------------------------------------------------------------" << endl;
    cout << "CLIENT #3: SELECT id, name FROM test.numbers LIMIT 10" << endl;
    cout << "--------------------------------------------------------------------------------" << endl;
    Client client3(ClientOptions().SetHost("localhost").SetPort(9300));
    client3.Select(
        "SELECT id, name FROM test.numbers LIMIT 10",
        [](const Block & block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i)
            {
                cout << block[0]->As<ColumnUInt64>()->At(i) << " " << block[1]->As<ColumnString>()->At(i) << endl;
            }
        });
    cout << "--------------------------------------------------------------------------------" << endl;


    cout << "--------------------------------------------------------------------------------" << endl;
    cout << "CLIENT #1: INSERT" << endl;
    cout << "--------------------------------------------------------------------------------" << endl;
    client1.Select(
        "SELECT COUNT(*) FROM test.numbers",
        [](const Block & block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i)
            {
                cout << "ROW COUNT BEFORE INSERT: " << block[0]->As<ColumnUInt64>()->At(i) << endl;
            }
        });

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(7);

        auto name = std::make_shared<ColumnString>();
        name->Append("one");
        name->Append("seven");

        block.AppendColumn("id", id);
        block.AppendColumn("name", name);

        client1.Insert("test.numbers", block);
    }

    client1.Select(
        "SELECT COUNT(*) FROM test.numbers",
        [](const Block & block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i)
            {
                cout << "ROW COUNT AFTER INSERT: " << block[0]->As<ColumnUInt64>()->At(i) << endl;
            }
        });
    cout << "--------------------------------------------------------------------------------" << endl;

    return 0;
}