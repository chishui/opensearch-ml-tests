# download file from a given url to a folder if the folder doesn not exist create
# it and download the file to the folder

import os
import urllib.request
import gzip
import shutil

urls = {
    "base_full.csr": "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/base_full.csr.gz",
    "base_small.csr": "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/base_small.csr.gz",
    "queries.dev.csr": "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/queries.dev.csr.gz",
    "brutal_array.txt": "https://do0ia2psryw9c.cloudfront.net/brutal_array.txt",
    "iv_array.txt": "https://do0ia2psryw9c.cloudfront.net/iv_array.txt",
    "iv_small_array.txt": "https://do0ia2psryw9c.cloudfront.net/iv_small_array.txt"

}

def download_with_progress(url, file_path):
    """Download a file from URL with progress bar."""
    def progress_callback(block_num, block_size, total_size):
        downloaded = block_num * block_size
        percent = min(100, downloaded * 100 / total_size)
        bar_length = 50
        filled_length = int(bar_length * downloaded // total_size)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        print(f'\r|{bar}| {percent:.1f}% {downloaded}/{total_size}', end='')
        if downloaded >= total_size:
            print()
    
    print(f"Downloading {os.path.basename(file_path)}...")
    urllib.request.urlretrieve(url, file_path, progress_callback)
    return file_path

def extract_gzip(gz_path, output_path):
    """Extract a gzip file to the specified output path."""
    print(f"Extracting {gz_path}...")
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Extracted to {output_path}")
    return output_path

def get_file(file_name):
    """Get a file by name, downloading and extracting if necessary."""
    folder_path = os.path.join(os.getcwd(), "data")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    # Get URL from the dictionary
    if file_name not in urls:
        raise ValueError(f"Unknown file: {file_name}")
    url = urls[file_name]
    
    # Check if the extracted file already exists
    output_path = os.path.join(folder_path, file_name)
    if os.path.exists(output_path):
        return output_path
    
    # Check if the compressed file already exists
    gz_file = url.split('/')[-1]
    gz_path = os.path.join(folder_path, gz_file)
    
    # Download if needed
    if not os.path.exists(gz_path):
        download_with_progress(url, gz_path)
    else:
        print(f"Compressed file {gz_file} already exists, skipping download")
    
    # Extract if it's a gzip file
    if gz_path.endswith('.gz'):
        extract_gzip(gz_path, output_path)
    
    return output_path

