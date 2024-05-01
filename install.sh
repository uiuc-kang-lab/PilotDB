# setup miniconda
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
export PATH=$~/miniconda3/bin:$PATH
~/miniconda3/bin/conda init bash
. ~/.bashrc

# setup python environment
conda create -y -n pilotdb
conda activate pilotdb
conda install -y -c conda-forge postgresql
conda install -y python=3.11
pip install -r requirements.txt
gdown 1CXdBfgbef4AxZYjzzMYWaRADzHZ5vtAU -O duckdb.whl
pip install duckdb.whl
pip install -e .
rm duckdb.whl
