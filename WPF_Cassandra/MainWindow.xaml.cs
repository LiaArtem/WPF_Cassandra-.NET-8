using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.IO;
using System.Globalization;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Configuration;
using System.Diagnostics;
using Cassandra;
using Cassandra.Mapping;
using Cassandra.Data.Linq;
using System.Xml.Linq;
using Newtonsoft.Json;

namespace WPF_Cassandra
{
    public class UserData
    {
        public Guid UserId { get; set; }
        private string? textValue;
        private int? intValue;
        private double? doubleValue;
        private Boolean? boolValue;
        private DateTime dateValue;
        private DateTime sysdateValue;
        public string TextValue { get => textValue ?? ""; set { textValue = value;}}
        public int? IntValue { get => intValue; set { intValue = value;}}
        public double? DoubleValue { get => doubleValue; set { doubleValue = value; }}
        public Boolean? BoolValue { get => boolValue; set { boolValue = value; }}
        public DateTime DateValue { get => dateValue; set { dateValue = value; }}
        public DateTime SysDateValue { get => sysdateValue; set { sysdateValue = (DateTime)value; }}                
    }
    
    public class JsonConnection
    {
        public string? Host;
        public int? Port;
        public string? User;
        public string? Pwd;
    }

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        readonly bool is_initialize = true;
        bool is_filter = false;
        private static Table<UserData>? UserData { get; set; }
        private Guid? DataGrig_Id;
        private ICluster? Cluster { get; set; }
        private ISession? Session { get; set; }
        private static JsonConnection? ParamJson;

        public MainWindow()
        {
            InitializeComponent();

            is_initialize = false;

            InitMapping();
            ParamJson = LoadJsonConnection();
            ReadDatabase();
            UpdateDatagrid();
        }

        public static JsonConnection LoadJsonConnection()
        {
            using StreamReader r = new("appsettings.json");
            string json = r.ReadToEnd();
            return JsonConvert.DeserializeObject<JsonConnection>(json);
        }

        // Инициализация Mapping
        private static void InitMapping()
        {
            //Set the Mapping Configuration            
            MappingConfiguration.Global.Define(
               new Map<UserData>()
                  .TableName("userdata")
                  .PartitionKey(u => u.UserId)
                  .Column(u => u.UserId, cm => cm.WithName("id")));
        }

        // Чтение базы данных
        private void ReadDatabase()
        {
            // build cluster connection
            if (ParamJson?.User == null || ParamJson?.Pwd == null)
            {
                Cluster = Cassandra.Cluster.Builder()
                           .AddContactPoint(ParamJson?.Host)
                           .WithPort(ParamJson?.Port ?? 9042)
                           .Build();
            }
            else
            {
                Cluster = Cassandra.Cluster.Builder()
                            .AddContactPoint(ParamJson?.Host)
                            .WithPort(ParamJson?.Port ?? 9042)
                            .WithCredentials(ParamJson?.User, ParamJson?.Pwd)
                            .Build();
            }
            // create session
            Session = Cluster.Connect();

            // prepare schema
            Session.Execute(new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS testdb WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }"));
            Session.Execute(new SimpleStatement("USE testdb"));
            Session.Execute(new SimpleStatement("CREATE TABLE IF NOT EXISTS userdata(id uuid, TextValue text, IntValue int, DoubleValue double, BoolValue boolean, DateValue Timestamp, SysDateValue Timestamp, PRIMARY KEY(id))"));

            //Create an instance of a Mapper from the session
            UserData = new Table<UserData>(Session);
        }

    private void UpdateDatagrid()
        {
            if (is_initialize == true) return;
            if (is_filter == false)
            {
                IEnumerable<UserData> usrs = UserData.Select(a => a).Execute();
                DataGrid1.ItemsSource = usrs;                 
            }
            else
            {                
                String m_value1 = value1.Text.ToString();
                String m_value2 = value2.Text.ToString();
                bool m_value1_bool;
                bool m_er;

                if (value_type.Text == "id")
                {
                    m_er = Guid.TryParse(m_value1, out Guid m_value1_guid);                    
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.UserId == m_value1_guid);
                }
                else if (value_type.Text == "text")
                {
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.TextValue.Contains(m_value1));
                }
                else if (value_type.Text == "int")
                {
                    m_er = int.TryParse(m_value1, out int m_value1_int);
                    m_er = int.TryParse(m_value2, out int m_value2_int);
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.IntValue >= m_value1_int && p.IntValue <= m_value2_int);
                }
                else if (value_type.Text == "double")
                {
                    m_er = double.TryParse(m_value1, out double m_value1_dbl);
                    m_er = double.TryParse(m_value2, out double m_value2_dbl);
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.DoubleValue >= m_value1_dbl && p.DoubleValue <= m_value2_dbl);
                }
                else if (value_type.Text == "bool")
                {
                    m_value1_bool = false;
                    if (m_value1.ToUpper() == "T" || m_value1.ToLower() == "true") m_value1_bool = true;
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.BoolValue == m_value1_bool);
                }
                else if (value_type.Text == "date")
                {
                    m_er = DateTime.TryParseExact(m_value1, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime m_value1_dat);
                    m_er = DateTime.TryParseExact(m_value2, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime m_value2_dat);
                    m_value2_dat = m_value2_dat.AddDays(1);
                    DataGrid1.ItemsSource = UserData.Select(a => a).Execute().Where(p => p.DateValue >= m_value1_dat && p.DateValue < m_value2_dat);
                }
            }
            this.DataContext = DataGrid1.ItemsSource;

            // Выделить сроку с курсором
            if (DataGrig_Id == null && DataGrid1.Items.Count > 0) DataGrig_Id = null;

            if (DataGrig_Id != null && DataGrid1.Items.Count > 0)
            {
                foreach (UserData drv in DataGrid1.ItemsSource)
                {
                    if (drv.UserId == DataGrig_Id)
                    {
                        DataGrid1.SelectedItem = drv;
                        DataGrid1.ScrollIntoView(drv);
                        DataGrid1.Focus();
                        break;
                    }
                }
            }
        }

        // изменение типа базы данных
        private void Database_type_SelectedIndexChanged(object sender, SelectionChangedEventArgs e)
        {
            //ComboBox comboBox = (ComboBox)sender;
            //ComboBoxItem selectedItem = (ComboBoxItem)comboBox.SelectedItem;            
            //try
            //{
            //    ReadDatabase();
            //    UpdateDatagrid();
            //}
            //catch (Exception ex)
            //{
            //    MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
            //    DataGrid1.ItemsSource = null;
            //}
        }

        // добавить запись
        private void Button_insertClick(object sender, RoutedEventArgs e)
        {
            AddWindow addWin = new(new UserData());
            if (addWin.ShowDialog() == true)
            {
                UserData ud = addWin.UserDataAdd;
                try
                {
                    ud.UserId = Guid.NewGuid();
                    ud.SysDateValue = DateTime.Now;
                    UserData?.Insert(ud).Execute();                    
                    UpdateDatagrid();
                }
                catch (Exception ex)
                {
                    MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                    DataGrid1.ItemsSource = null;
                }
            }
        }

        // изменить запись
        private void Button_updateClick(object sender, RoutedEventArgs e)
        {
            // если ни одного объекта не выделено, выходим
            if (DataGrid1.SelectedItem == null) return;
            // получаем выделенный объект
            if (DataGrid1.SelectedItem is UserData ud)
            {
                AddWindow addWin = new(new UserData
                {
                    UserId = ud.UserId,
                    TextValue = ud.TextValue,
                    IntValue = ud.IntValue,
                    DoubleValue = ud.DoubleValue,
                    BoolValue = ud.BoolValue,
                    DateValue = ud.DateValue
                });

                if (addWin.ShowDialog() == true)
                {
                    // получаем измененный объект                
                    try
                    {
                        // проверка на неконтролируемое изменение
                        DateTime selectedSysDate = UserData.Where(u => u.UserId == ud.UserId).Execute().Select(u => u.SysDateValue).First();
                        if (selectedSysDate != ud.SysDateValue)
                        {
                            MessageBox("Неконтролируемое изменение, попробуйте повторно", System.Windows.MessageBoxImage.Error);
                            DataGrid1.ItemsSource = null;
                            return;
                        }

                        UserData.Where(u => u.UserId == ud.UserId)
                                .Select(u => new UserData
                                {
                                    TextValue = addWin.UserDataAdd.TextValue,
                                    IntValue = addWin.UserDataAdd.IntValue,
                                    DoubleValue = addWin.UserDataAdd.DoubleValue,
                                    BoolValue = addWin.UserDataAdd.BoolValue,
                                    DateValue = addWin.UserDataAdd.DateValue,
                                    SysDateValue = DateTime.Now
                                })
                                .Update()
                                .Execute();
                        UpdateDatagrid();
                        MessageBox("Запись обновлена");
                    }
                    catch (Exception ex)
                    {
                        MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                        DataGrid1.ItemsSource = null;
                    }
                }
            }
        }

        // удалить запись
        private void Button_deleteClick(object sender, RoutedEventArgs e)
        {
            // если ни одного объекта не выделено, выходим
            if (DataGrid1.SelectedItem == null) return;

            MessageBoxResult result = System.Windows.MessageBox.Show("Удалить запись ???", "Сообщение", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Question);
            switch (result)
            {
                case MessageBoxResult.Yes:
                    // получаем выделенный объект
                    if (DataGrid1.SelectedItem is UserData ud)
                    {
                        try
                        {
                            UserData.Where(u => u.UserId == ud.UserId)
                                  .Delete()
                                  .Execute();
                            UpdateDatagrid();
                        }
                        catch (Exception ex)
                        {
                            MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                            DataGrid1.ItemsSource = null;
                        }                        
                    }
                    MessageBox("Запись удалена");
                    break;
                case MessageBoxResult.No:
                    break;
            }
        }

        // обновить запись
        private void Button_selectClick(object sender, RoutedEventArgs e)
        {            
            try
            {
                ReadDatabase();
                UpdateDatagrid();
            }
            catch (Exception ex)
            {
                MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                DataGrid1.ItemsSource = null;
            }
        }

        private readonly SolidColorBrush hb = new(Colors.MistyRose);
        private readonly SolidColorBrush nb = new(Colors.AliceBlue);
        private void DataGrid1_LoadingRow(object sender, DataGridRowEventArgs e)
        {            
            if ((e.Row.GetIndex() + 1) % 2 == 0)
                e.Row.Background = hb;
            else
                e.Row.Background = nb;

            // А можно в WPF установить - RowBackground - для нечетных строк и AlternatingRowBackground
        }

        // вывод диалогового окна
        public static void MessageBox(String infoMessage, MessageBoxImage mImage = System.Windows.MessageBoxImage.Information)
        {
            System.Windows.MessageBox.Show(infoMessage, "Сообщение", System.Windows.MessageBoxButton.OK, mImage);
        }
        
        private void DataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                var row_list = (UserData)DataGrid1.SelectedItem;
                if (row_list != null)
                    DataGrig_Id = row_list.UserId;
            }
            catch
            {
                DataGrig_Id = null;
            }
        }

        private void DataGrid_MouseDoubleClick(object sender, RoutedEventArgs e)
        {
            Button_updateClick(sender, e);
        }

        // применить фильтр
        private void Button_findClick(object sender, RoutedEventArgs e)
        {
            is_filter = true;            
            try
            {
                ReadDatabase();
                UpdateDatagrid();
            }
            catch (Exception ex)
            {
                MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                DataGrid1.ItemsSource = null;
            }
        }

        // отменить фильтр
        private void Button_find_cancelClick(object sender, RoutedEventArgs e)
        {
            is_filter = false;
            value1.Text = "";
            value2.Text = "";            
            try
            {
                ReadDatabase();
                UpdateDatagrid();
            }
            catch (Exception ex)
            {
                MessageBox(ex.Message, System.Windows.MessageBoxImage.Error);
                DataGrid1.ItemsSource = null;
            }
        }

        // изменение типа данных
        private void Value_type_SelectedIndexChanged(object sender, SelectionChangedEventArgs e)
        {
            if (is_initialize == true) return;

            ComboBox comboBox = (ComboBox)sender;
            ComboBoxItem selectedItem = (ComboBoxItem)comboBox.SelectedItem;
            String? value_type = selectedItem.Content.ToString();

            if (value_type == "id") { value2.IsEnabled = false; value2.Text = ""; }
            else if (value_type == "text") { value2.IsEnabled = false; value2.Text = ""; }
            else if (value_type == "int") value2.IsEnabled = true;
            else if (value_type == "double") value2.IsEnabled = true;
            else if (value_type == "bool") { value2.IsEnabled = false; value2.Text = ""; }
            else if (value_type == "date") value2.IsEnabled = true;
        }

        // изменение фокуса на value2
        private void Value2_GotKeyboardFocus(object sender, EventArgs e)
        {
            if (value1.Text != "") value2.Text = value1.Text;
        }
    }

}

