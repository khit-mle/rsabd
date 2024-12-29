import io
import os

import luigi
import magic
import pandas as pd
import patoolib
import requests


class DownloadDataset(luigi.Task):
    """Download dataset from NCBI GEO and verify it's an archive."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def output(self):
        return luigi.LocalTarget(f"{self.dataset_name}.archive")

    def run(self):
        # Download the file
        url = f'https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar'
        print(f"Downloading from {url}")
        
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 404:
            # Try alternative URL format
            url = f'https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}.tar.gz'
            print(f"First URL failed, trying {url}")
            response = requests.get(url, stream=True, timeout=30)
            
        response.raise_for_status()
        
        # Get total file size
        total_size = int(response.headers.get('content-length', 0))
        print(f"Total file size: {total_size/1024/1024:.2f} MB")
        
        # Download with progress
        with open(self.output().path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        percent = (downloaded / total_size) * 100
                        print(f"\rDownloaded: {downloaded/1024/1024:.2f}MB ({percent:.1f}%)", end='')
            print("\nDownload completed")

        # Verify it's an archive
        if not self._is_archive(self.output().path):
            # Clean up and raise error
            os.remove(self.output().path)
            raise ValueError(f"Downloaded file {self.dataset_name} is not an archive")

    def _is_archive(self, file_path):
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(file_path)
        # add here new archive mime types for detection
        archive_mime_types = [
            # ZIP formats
            'application/zip',
            'application/x-zip',
            
            # TAR formats
            'application/tar',
            'application/x-tar',
            
            # GZIP formats
            'application/gzip',
            'application/x-gzip',
            
            # BZIP2 formats
            'application/bzip2',
            'application/x-bzip2',
            
            # Other compression formats
            'application/x-rar-compressed',
            'application/x-7z-compressed',
            'application/xz',
            'application/x-xz',
            'application/lzip',
            'application/x-lzip',
            'application/lzma',
            'application/x-lzma',
            'application/zstd',
            'application/x-zstd',
            
            # Unix archive formats
            'application/x-cpio',
            'application/x-archive',
            'application/x-shar'
        ]
        return file_type in archive_mime_types


class ExtractArchive(luigi.Task):
    """Extract archives recursively."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def requires(self):
        return DownloadDataset(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(f"dataset_{self.dataset_name}")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)
        self._extract_recursive(self.input().path, self.output().path)

    def _extract_recursive(self, archive_path, output_dir):
        try:
            patoolib.extract_archive(archive_path, outdir=output_dir)

            # Recursively check extracted contents for more archives
            for root, _, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self._is_archive(file_path):
                        # Create subdirectory for nested archive
                        subdir = os.path.join(root, f"{file}_extracted")
                        os.makedirs(subdir, exist_ok=True)
                        self._extract_recursive(file_path, subdir)
                        # Remove the archive after extraction
                        os.remove(file_path)

        except Exception as e:
            raise RuntimeError(f"Failed to extract {archive_path}. Error: {e}")

    def _is_archive(self, file_path):
        return DownloadDataset._is_archive(self, file_path)


class ProcessTextFiles(luigi.Task):
    """Process text files into dataframes."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def requires(self):
        return ExtractArchive(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(f"processed_{self.dataset_name}")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)

        # Process all txt files in the extracted directory
        for root, _, files in os.walk(self.input().path):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    try:
                        dfs = self._parse_text_file(file_path)
                        self._save_dataframes(dfs, file)
                        # Remove the original txt file after successful processing
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Failed to process {file_path}. Error: {e}")

    def _parse_text_file(self, file_path):
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():  # noqa: E741
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')
        return dfs

    def _save_dataframes(self, dfs, original_filename):
        for key, df in dfs.items():
            # Special handling for Probes dataframe
            if key == 'Probes':
                # Save full version
                df.to_csv(
                    os.path.join(self.output().path, f"{original_filename}_{key}_full.csv"),
                    index=False
                )
                # Save short version
                columns_to_remove = [
                    'Definition', 'Ontology_Component', 'Ontology_Process',
                    'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id',
                    'Probe_Sequence'
                ]
                short_df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
                short_df.to_csv(
                    os.path.join(self.output().path, f"{original_filename}_{key}_short.csv"),
                    index=False
                )
            else:
                df.to_csv(
                    os.path.join(self.output().path, f"{original_filename}_{key}.csv"),
                    index=False
                )


class GEOPipeline(luigi.WrapperTask):
    """Main pipeline task that processes a GEO dataset."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def requires(self):
        return ProcessTextFiles(dataset_name=self.dataset_name)


if __name__ == '__main__':
    luigi.run()
