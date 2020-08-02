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

namespace ConsoleAppDapper
{
    class Program
    {
        static void Main(string[] args)
        {
            string connStr = @"Data Source=TORPC\TOR;Initial Catalog=dapper_demo;User ID=sa;Password=1";

            string mySqlConnStr = @"server=127.0.0.1;user id=root;password=123456;persistsecurityinfo=True;database=world";

            var dh = new DapperHelper(connStr);

            var tst=dh.QueryDataSet("SELECT * FROM Person  WHERE Id = @Id; ", new { Id = 1 });


            //dh.Execute("INSERT INTO Person (Name) VALUES (@Name);", new[] { new Person() {Name="路易" } },(i,tran)=>{
            //    tran.Commit();
            //    return i;
            //});
            var personDictionary = new Dictionary<int, Person>();
            var pp=dh.Query("SELECT * FROM Person  WHERE Id = @Id; ",new { Id=1});
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
            var mySqlDh = DapperHelper.GetMySql(mySqlConnStr);

            //IEnumerable<dynamic> ttt1=null;
            //IEnumerable<dynamic> ttt2;
            //mySqlDh.QueryMultiple<DataTable,dynamic>("SELECT * FROM world.city;SELECT * FROM world.city;",(t1,t2)=>{

            //    ttt1 = t1;
            //    ttt2 = t2;
            //});
            //var eR=Insert(connStr, new[] { new Person() { Name = "小红" }, new Person() { Name = "小蓝" } });



            var ds = mySqlDh.QueryDataSet("SELECT * FROM world.city;SELECT * FROM world.country;");
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
}
