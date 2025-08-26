# tests/test_data_tasks.py
import unittest
from unittest.mock import patch, MagicMock
from src.tasks.data_tasks import collect_system_metrics, save_data_to_storage

class TestCollectSystemMetrics(unittest.TestCase):
    """
    Testes para a tarefa de coleta de métricas do sistema.
    """
    @patch('src.tasks.data_tasks.psutil')
    def test_collect_system_metrics_returns_correct_data(self, mock_psutil):
        """
        Verifica se a função de coleta retorna um dicionário com os campos esperados e tipos corretos.
        """
        # Configura as respostas simuladas
        mock_psutil.cpu_percent.return_value = 50.0
        mock_psutil.virtual_memory.return_value = MagicMock(percent=75.0)
        mock_psutil.disk_usage.return_value = MagicMock(percent=40.0)
        mock_psutil.net_io_counters.return_value = MagicMock(bytes_sent=1000, bytes_recv=2000)
        mock_psutil.cpu_freq.return_value = MagicMock(current=2500)

        metrics = collect_system_metrics()

        self.assertIsInstance(metrics, dict)
        self.assertEqual(metrics['cpu_percent'], 50.0)
        self.assertEqual(metrics['mem_percent'], 75.0)
        self.assertEqual(metrics['disk_percent'], 40.0)
        self.assertEqual(metrics['network_sent_bytes'], 1000)
        self.assertEqual(metrics['network_recv_bytes'], 2000)
        self.assertEqual(metrics['cpu_freq_ghz'], 2.5)
        
    @patch('src.tasks.data_tasks.psutil.cpu_percent', side_effect=Exception("Simulated failure"))
    def test_collect_system_metrics_handles_failure(self, mock_cpu_percent):
        """
        Verifica se a função lida com falhas durante a coleta e retorna None.
        """
        metrics = collect_system_metrics()
        self.assertIsNone(metrics)


class TestSaveDataToStorage(unittest.TestCase):
    """
    Testes para a tarefa de salvamento de dados no MinIO.
    """
    @patch('src.tasks.data_tasks.Minio')
    @patch('src.tasks.data_tasks.pd.DataFrame')
    @patch('src.tasks.data_tasks.pa.BufferOutputStream')
    @patch('src.tasks.data_tasks.pq')
    @patch('src.tasks.data_tasks.pa')
    def test_save_data_to_storage_calls_minio_correctly(
        self, mock_pa, mock_pq, mock_buffer_output_stream, mock_dataframe, mock_minio_client
    ):
        """
        Verifica se a função de salvamento interage com o cliente MinIO corretamente.
        """
        mock_client_instance = mock_minio_client.return_value
        mock_client_instance.bucket_exists.return_value = True
        
        sample_metrics = {
            "timestamp": "2025-08-25T20:15:53.934649",
            "cpu_percent": 13.6,
            "mem_percent": 88.5,
            "disk_percent": 35.6,
        }
        
        mock_table = MagicMock()
        mock_pa.Table.from_pandas.return_value = mock_table
        
        mock_buffer = mock_buffer_output_stream.return_value
        mock_buffer.tell.return_value = 1000

        with patch('src.tasks.data_tasks.pq.write_table', return_value=None):
            success = save_data_to_storage(sample_metrics)

            self.assertTrue(success)
            mock_client_instance.put_object.assert_called_once_with(
                "landing",
                unittest.mock.ANY, # Verifica que o argumento é uma string, mas não se importa com o valor
                mock_buffer,
                length=1000,
                content_type='application/parquet'
            )
            mock_buffer.seek.assert_called_once_with(0)

if __name__ == '__main__':
    unittest.main()