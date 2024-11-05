import unittest
from pyspark.sql import SparkSession
from src.funciones import iniciar_spark, load_data, preprocess_census_data, preprocess_crimes_data

class DatosTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = iniciar_spark("TestDatos")
        cls.census_path = "data/acs2017_census_tract_data.csv"
        cls.crimes_path = "data/estimated_crimes_1979_2019.csv"

    def test_load_data(self):
        """
        Prueba que los datos se cargan sin errores.
        """
        census_df, crimes_df = load_data(self.spark, self.census_path, self.crimes_path)
        self.assertIsNotNone(census_df)
        self.assertIsNotNone(crimes_df)

    def test_preprocess_census_data(self):
        """
        Verifica que el DataFrame del censo esté limpio después de aplicar el preprocesamiento.
        """
        census_df, _ = load_data(self.spark, self.census_path, self.crimes_path)
        census_df = preprocess_census_data(census_df)
        
        # Confirmar que no hay valores nulos
        self.assertEqual(census_df.na.drop().count(), census_df.count())

    def test_preprocess_crimes_data(self):
        """
        Verifica que la columna 'Year' ha sido eliminada y que no hay valores nulos en el DataFrame de crímenes.
        """
        _, crimes_df = load_data(self.spark, self.census_path, self.crimes_path)
        crimes_df = preprocess_crimes_data(crimes_df)

        # Verificar que la columna 'Year' no está presente
        self.assertNotIn("Year", crimes_df.columns)
        
        # Confirmar que no hay valores nulos
        self.assertEqual(crimes_df.na.drop().count(), crimes_df.count())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
