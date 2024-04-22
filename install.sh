mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
export PATH=$~/miniconda3/bin:$PATH
conda init bash
. ~/.bashrc
conda create -n pilotdb
conda activate pilotdb
conda install -y -c conda-forge postgresql
conda install -y python=3.10
pip install -r requirements.txt
pip install -e .