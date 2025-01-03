using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BotVidas
{
    public partial class Form1 : Form
    {

        private cBotVidas BotVidas;

        public Form1()
        {
            InitializeComponent();
            BotVidas = new cBotVidas(dataGridViewLog);
            labelTiempo.Text = BotVidas.GetHoursConfiguration();
            this.Cancelar.Enabled = false;
        }

        private void label1_Click(object sender, EventArgs e)
        {

        }

        private async void button3_Click(object sender, EventArgs e)
        {

            if(int.TryParse(textBox1.Text, out int result))
            {
                if (result > 0 && result < 100)
                {
                    await Task.Run(() =>
                    {
                        BotVidas.ChangeTime(result);
                        this.Invoke((MethodInvoker)delegate
                        {
                            labelTiempo.Text = textBox1.Text;
                            textBox1.Text = "";
                        });
                    });
                }
                else
                {
                    MessageBox.Show("Error en los valores de entrada. El rango es de 1 a 100 horas", "Error al cambiar hora", MessageBoxButtons.OK, MessageBoxIcon.Information);

                }
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            this.button2.Enabled = false;
            this.Cancelar.Enabled = true;
            BotVidas.principal();

        }


        private void labelUltimoSMS_Click(object sender, EventArgs e)
        {

        }

        private void dataGridView1_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {
        }

        private void btnCancelar_Click(object sender, EventArgs e)
        {
           
        }

        private void Cancelar_Click(object sender, EventArgs e)
        {
            this.button2.Enabled = true;
            this.Cancelar.Enabled = false;
            BotVidas.CancelProcess();
        }

        private void textBox1_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (!char.IsDigit(e.KeyChar) && e.KeyChar != (char)Keys.Back && e.KeyChar != '-')
            {
                e.Handled = true;  
            }
        }

        private void label3_Click(object sender, EventArgs e)
        {

        }

        private void labelTiempo_Click(object sender, EventArgs e)
        {

        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }
    }
}
