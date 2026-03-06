from setuptools import setup, find_packages

setup(name='MOEXPy',
      version='2016.03.01',  # Внутренняя версия формата <Год>.<Месяц>.<Номер>
      author='Чечет Игорь Александрович',
      description='Библиотека-обертка, которая позволяет работать с Algopack API Московской Биржи из Python',
      url='https://github.com/cia76/MOEXPy',
      packages=find_packages(),
      install_requires=[
            'keyring',  # Безопасное хранение торгового токена
            'requests',  # Запросы/ответы через HTTP API
      ],
      python_requires='>=3.12',
      )
