using FstDapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Runtime.InteropServices.ComTypes;

namespace ConsoleAppDapper
{
    class Program
    {
        static void Main(string[] args)
        {
            string connStr = @"Data Source=TORPC\TOR;Initial Catalog=dapper_demo;User ID=sa;Password=1";

            // SqlServer
            var dh = new DapperHelper(connStr);
            //查询

            #region 返回DataTable
            //全表查询
            DataTable dt1 = dh.QueryDataTable("SELECT * FROM Person; ");
            //指定返回表的别名，如果不指定则返回空表名，所有返回DataTable的方法都提供tablename参数
            DataTable dt1_1 = dh.QueryDataTable("SELECT * FROM Person; ", tablename: "指定别名");
            //带参数查询
            DataTable dt2 = dh.QueryDataTable("SELECT * FROM Person WHERE Id = @Id; ", new { Id = 1 });
            //In查询
            DataTable dt3 = dh.QueryDataTable("SELECT * FROM Person WHERE Id IN @Id; ", new { Id = new[] { 1, 2 } });
            //In查询，可使用字符串
            DataTable dt4 = dh.QueryDataTable("SELECT * FROM Person WHERE Id IN @Id; ", new { Id = new[] { "1", "2" } });
            //使用存储过程查询
            DataTable dt5 = dh.QueryDataTable("GetPerson", commandType: CommandType.StoredProcedure);
            //使用存储过程查询
            DataTable dt6 = dh.QueryDataTableProc("GetPerson");
            //使用存储过程查询，带参数
            DataTable dt7 = dh.QueryDataTableProc("GetPersonById", new { Id = 1 });

            #endregion

            #region 返回DataSet
            //全表查询
            DataSet ds1 = dh.QueryDataSet("SELECT * FROM Person; SELECT * FROM Son;");
            //全表查询,按顺序将返回表起别名，如果不传入tablenames返回的DataSet中表将按照Table1、Table2..进行命名
            DataSet ds1_1 = dh.QueryDataSet("SELECT * FROM Person; SELECT * FROM Son; ", tablenames: new[] { "Person1", "Son1" });
            //带参数查询
            DataSet ds2 = dh.QueryDataSet("SELECT * FROM Person WHERE Id = @Id; SELECT * FROM Son WHERE PId = @PId; ", new { Id = 1, PId = 1 });
            //In查询
            DataSet ds3 = dh.QueryDataSet("SELECT * FROM Person WHERE Id IN @Id; SELECT * FROM Son WHERE PId IN @PId; ", new { Id = new[] { 1, 2 }, PId = new[] { 1, 2 } });
            //使用存储过程
            DataSet ds4 = dh.QueryDataSet("GetPerson", commandType: CommandType.StoredProcedure);
            DataSet ds5 = dh.QueryDataSetProc("GetPerson");
            DataSet ds6 = dh.QueryDataSetProc("GetPersonById", new { Id = 2 });
            #endregion

            #region 动态类型查询
            //全表查询
            var q1_1 = dh.Query("SELECT * FROM Person; ");
            //存储过程
            var q1_1_1 = dh.Query("GetPerson", commandType: CommandType.StoredProcedure);
            var q1_1_2 = dh.QueryProc("GetPerson");
            //带参数查询
            var q1_2 = dh.Query("SELECT * FROM Person WHERE Id = @Id; ", new { Id = 1 });
            //In查询
            var q1_3 = dh.Query("SELECT * FROM Person WHERE Id In @Id; ", new { Id = new[] { 1, 2 } });
            //查询第一条数据（为Query的特化版，无需再取第0条，常用于确定只有一条数据的情况）
            var q1_4 = dh.QueryFirstOrDefault("SELECT * FROM Person; ");
            var q1_5 = dh.QueryFirstOrDefault("SELECT * FROM Person WHERE Id = @Id; ", new { Id = 1 });
            #endregion

            #region 强类型查询
            //全表查询
            IEnumerable<Person> q2_1 = dh.Query<Person>("SELECT * FROM Person; ");
            //存储过程
            IEnumerable<Person> q2_1_1 = dh.Query<Person>("GetPerson", commandType: CommandType.StoredProcedure);
            IEnumerable<Person> q2_1_2 = dh.QueryProc<Person>("GetPerson");
            //带参数查询
            IEnumerable<Person> q2_2 = dh.Query<Person>("SELECT * FROM Person WHERE Id = @Id; ", new { Id = 1 });
            //In查询
            IEnumerable<Person> q2_3 = dh.Query<Person>("SELECT * FROM Person WHERE Id In @Id; ", new { Id = new[] { 1, 2 } });
            //查询第一条数据（为Query的特化版，无需再取第0条，常用于确定只有一条数据的情况）
            Person q2_4 = dh.QueryFirstOrDefault<Person>("SELECT * FROM Person; ");
            Person q2_5 = dh.QueryFirstOrDefault<Person>("SELECT * FROM Person WHERE Id = @Id; ", new { Id = 1 });
            #endregion

            #region 返回第一行第一个值
            object id1 = dh.ExecuteScalar("SELECT Id FROM Person; ");
            int id2 = dh.ExecuteScalar<int>("SELECT Id FROM Person; ");
            object name1 = dh.ExecuteScalar("SELECT Name FROM Person WHERE Id=@Id; ", new { Id = 1 });
            string name2 = dh.ExecuteScalar<string>("SELECT Name FROM Person WHERE Id=@Id; ", new { Id = 1 });
            #endregion

            #region 多表查询
            //动态查询
            dynamic dyPerson;
            dynamic dySon;
            dh.QueryMultiple<dynamic, dynamic>("SELECT * FROM Person; SELECT * FROM Son; ", (persons, sons) =>
            {
                dyPerson = persons;
                dySon = sons;
            });
            //强类型查询
            IEnumerable<Person> person1;
            IEnumerable<Son> son1;
            dh.QueryMultiple<Person, Son>("SELECT * FROM Person; SELECT * FROM Son; ", (persons, sons) =>
            {
                person1 = persons;
                son1 = sons;
            });
            #endregion

            #region 强类型一对多查询（可不看）
            var personDictionary = new Dictionary<int, Person>();
            var p = dh.Query<Person, string, Person>("SELECT a.Id,a.Name,b.Name FROM Person a Left JOIN Son b ON a.Id=b.Pid WHERE a.Id=@Id; ",
                (person, son) =>
                {

                    Person personEntry;
                    if (!personDictionary.TryGetValue(person.Id, out personEntry))
                    {
                        personEntry = person;
                        personEntry.Sons = new string[] { };
                        personDictionary.Add(personEntry.Id, personEntry);
                    }

                    if (!string.IsNullOrWhiteSpace(son))
                    {
                        personEntry.Sons = personEntry.Sons.Union(new[] { son });
                    }
                    return personEntry;
                }, new { Id = 1 }, "Name");
            #endregion

            #region 增删改（带事务）
            //返回【小sdf234sf】的字符串方法
            Func<string> getName = () => "小" + Guid.NewGuid().ToString().Replace("-", "");

            //增 单个 匿名
            dh.Execute("INSERT INTO Person (Name) VALUES (@Name)", new { Name = getName() });
            //增 多个 匿名
            dh.Execute("INSERT INTO Person (Name) VALUES (@Name)", new[] {
                new { Name= getName() },
                new { Name= getName() }
            });

            //增 单个 强类型
            dh.Execute("INSERT INTO Person (Name) VALUES (@Name)", new Person() { Name = getName() });
            //增 多个 强类型
            dh.Execute("INSERT INTO Person (Name) VALUES (@Name)", new[] {
                new Person() { Name = getName() },
                new Person() { Name = getName() }
            });


            //改 单个 匿名
            dh.Execute("UPDATE Person SET Name=@Name WHERE Id=@Id", new { Name = getName(), Id = 3 });
            //改 多个 匿名
            dh.Execute("UPDATE Person SET Name=@Name WHERE Id=@Id", new[] {
                new { Name = getName(), Id = 3 },
                new { Name = getName(), Id = 4 }
            });

            //改 单个 强类型
            dh.Execute("UPDATE Person SET Name=@Name WHERE Id=@Id", new Person() { Name = getName(), Id = 3 });
            //改 多个 强类型
            dh.Execute("UPDATE Person SET Name=@Name WHERE Id=@Id", new[] {
                new Person() { Name = getName(), Id = 3 },
                new Person(){ Name = getName(), Id = 4 }
            });


            //删
            dh.Execute("DELETE FROM Person WHERE Id>@Id", new { Id = 5 });
            dh.Execute("DELETE FROM Person WHERE Id=@Id", new[]{
                new { Id = 5 } ,
                new { Id = 6 }
            });

            dh.Execute("DELETE FROM Person WHERE Id>@Id", new Person() { Id = 5 });
            dh.Execute("DELETE FROM Person WHERE Id=@Id", new[]{
                new Person() { Id = 5 } ,
                new Person() { Id = 6 }
            });

            //使用In删除
            dh.Execute("DELETE FROM Person WHERE Id IN @Id", new { Id = new[] { 5, 6 } });

            #endregion

            #region 使用MySql
            //使用MySql示例
            string mySqlConnStr = @"server=127.0.0.1;user id=root;password=123456;persistsecurityinfo=True;database=world";
            //实例化对象方法一
            var mySqlDh1 = new DapperHelper(mySqlConnStr, DataBaseType.MySql);
            //实例化对象方法二
            var mySqlDh2 = DapperHelper.GetMySql(mySqlConnStr);

            DataTable mysql_dt = mySqlDh1.QueryDataTable("SELECT * FROM world.city; ");
            DataSet mysql_ds = mySqlDh1.QueryDataSet("SELECT * FROM world.city; SELECT * FROM world.country; ");
            #endregion

            Console.WriteLine();
            Console.ReadLine();
        }
    }



    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public IEnumerable<string> Sons { get; set; }
    }

    public class Son
    {
        public int PId { get; set; }
        public string Name { get; set; }
    }
}
