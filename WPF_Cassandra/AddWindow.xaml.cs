using System;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Input;

namespace WPF_Cassandra
{
    /// <summary>
    /// Логика взаимодействия для AddWindow.xaml
    /// </summary>
    public partial class AddWindow : Window
    {
        public UserData UserDataAdd { get; private set; }

        public AddWindow(UserData ud)
        {
            InitializeComponent();

            UserDataAdd = ud;                        
            // если класс пустой
            if (UserDataAdd.DateValue == DateTime.Parse("01.01.0001 0:00:00")) 
            {                
                this.col_date.SelectedDate = DateTime.Today;
                UserDataAdd.DateValue = DateTime.Today;
            }            
            // если класс пустой
            if (UserDataAdd.BoolValue == null)
            {                
                this.col_yes_no.IsChecked = false;
                UserDataAdd.BoolValue = false;
            }
            // если класс пустой
            if (UserDataAdd.TextValue == null)
            {
                UserDataAdd.TextValue = "";
            }
            this.DataContext = UserDataAdd;            

            // подписываем textBox на событие PreviewTextInput, с помощью которого можно обрабатывать вводимый текст
            col_int.PreviewTextInput += new TextCompositionEventHandler(TextBox_PreviewTextInput_Int);
            col_num.PreviewTextInput += new TextCompositionEventHandler(TextBox_PreviewTextInput_Float);
        }

        // Сохранить
        private void Button_saveClick(object sender, RoutedEventArgs e)
        {
            this.DialogResult = true;
        }
       
        private void TextBox_PreviewTextInput_Float(object sender, TextCompositionEventArgs e)
        {
            string inputSymbol = e.Text.ToString(); // можно вводить цифры и точку
            if (!RegexFloat().Match(inputSymbol).Success)
            {
                e.Handled = true;
            }
        }
        private void TextBox_PreviewTextInput_Int(object sender, TextCompositionEventArgs e)
        {
            string inputSymbol = e.Text.ToString(); // можно вводить цифры
            if (!RegexInt().Match(inputSymbol).Success)
            {
                e.Handled = true;
            }
        }

        [GeneratedRegex("[0-9]")]
        private static partial Regex RegexInt();

        [GeneratedRegex("[0-9]|\\.")]
        private static partial Regex RegexFloat ();
    }
}
