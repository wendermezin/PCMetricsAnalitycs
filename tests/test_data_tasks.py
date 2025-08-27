# tests/test_data_tasks.py
import unittest
from unittest.mock import patch, MagicMock

# Import the actual functions
from src.tasks.data_tasks import collect_system_metrics, save_data_to_storage

class TestCollectSystemMetrics(unittest.TestCase):
    # ... (collect_system_metrics tests are correct, no changes needed) ...
    @patch('src.tasks.data_tasks.psutil')
    def test_collect_system_metrics_returns_correct_data(self, mock_psutil):
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
    @patch('src.tasks.data_tasks.io')
    def test_save_data_to_storage_calls_minio_correctly(
        self, mock_io, mock_pa, mock_pq, mock_buffer_output_stream, mock_dataframe, mock_minio_client
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
        
        # Mocks para o pipeline de dados
        mock_pybytes = MagicMock()
        mock_pybytes.__len__.return_value = 1000
        
        mock_buffer_instance = mock_buffer_output_stream.return_value
        mock_buffer_instance.getvalue.return_value.to_pybytes.return_value = mock_pybytes

        mock_io.BytesIO.return_value = MagicMock()
        mock_table = MagicMock()
        mock_pa.Table.from_pandas.return_value = mock_table

        # Chama a função que estamos testando
        result = save_data_to_storage(sample_metrics)

        # Asserções
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("s3a://landing/pc_metrics"))
        mock_client_instance.put_object.assert_called_once_with(
            "landing",
            unittest.mock.ANY, # It's better to use ANY for dynamic path
            mock_io.BytesIO.return_value, # Use the mock object returned by BytesIO
            length=1000,
            content_type='application/parquet'
        )
    
    @patch('src.tasks.data_tasks.Minio', side_effect=Exception("Simulated MinIO connection error"))
    def test_save_data_to_storage_handles_minio_failure(self, mock_minio):
        """
        Verifica se a função lida com falhas no cliente Minio e retorna None.
        """
        sample_metrics = {"timestamp": "2025-08-25T20:15:53.934649"}
        result = save_data_to_storage(sample_metrics)
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()