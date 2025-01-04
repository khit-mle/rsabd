import io
import os
import shutil

import luigi
import magic
import pandas as pd
import patoolib
import requests


class DownloadDataset(luigi.Task):
    """Download dataset from NCBI GEO and verify it's an archive."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def output(self):
        # Ensure dataset directory exists
        os.makedirs('dataset', exist_ok=True)
        return luigi.LocalTarget(f"dataset/{self.dataset_name}_RAW.tar")

    def _download_with_progress(self, url, output_path, timeout=30):
        """Download file with progress reporting."""
        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            print(f"Total file size: {total_size / 1024 / 1024:.2f} MB")

            with open(output_path, 'wb') as file:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            percent = (downloaded / total_size) * 100
                            print(f"\rDownloaded: {downloaded / 1024 / 1024:.2f}MB ({percent:.1f}%)", end='', flush=True)
                print("\nDownload completed!")
            return True
        except requests.exceptions.RequestException:
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    def run(self):
        # Clean up and recreate dataset directory
        if os.path.exists('dataset'):
            shutil.rmtree('dataset')
        os.makedirs('dataset')

        # Try direct download first
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        print(f"Trying direct download from {url}")
        if self._download_with_progress(url, self.output().path):
            if self._is_archive(self.output().path):
                return
            os.remove(self.output().path)

        # Try FTP URL with _RAW.tar
        url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        print(f"Direct download failed. Trying FTP URL: {url}")
        if self._download_with_progress(url, self.output().path):
            if self._is_archive(self.output().path):
                return
            os.remove(self.output().path)

        # Try alternative FTP URL with .tar.gz
        url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}.tar.gz"
        print(f"First FTP URL failed. Trying alternative FTP URL: {url}")
        if self._download_with_progress(url, self.output().path):
            if self._is_archive(self.output().path):
                return
            os.remove(self.output().path)

        raise ValueError(f"Failed to download archive for {self.dataset_name} from any source")

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
        return luigi.LocalTarget(f"dataset/{self.dataset_name}_RAW_extracted_and_processed")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)
        self._extract_recursive(self.input().path, self.output().path)

        # Clean up empty directories
        for root, dirs, files in os.walk(self.output().path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if not os.listdir(dir_path):  # if directory is empty
                    os.rmdir(dir_path)

    def _extract_recursive(self, archive_path, output_dir):
        try:
            # Extract to a temporary directory first
            temp_dir = os.path.join(output_dir, 'temp_extract')
            os.makedirs(temp_dir, exist_ok=True)
            patoolib.extract_archive(archive_path, outdir=temp_dir)

            # Move files to their final location
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self._is_archive(file_path):
                        # Create subdirectory based on the original filename without extension
                        base_name = os.path.splitext(os.path.splitext(file)[0])[0]  # Remove both .txt.gz
                        subdir = os.path.join(output_dir, base_name)
                        os.makedirs(subdir, exist_ok=True)
                        self._extract_recursive(file_path, subdir)
                        os.remove(file_path)
                    else:
                        # Create subdirectory based on the original filename without extension
                        base_name = os.path.splitext(file)[0]
                        file_dir = os.path.join(output_dir, base_name)
                        os.makedirs(file_dir, exist_ok=True)
                        # Move non-archive files to their directory
                        dest_path = os.path.join(file_dir, file)
                        shutil.move(file_path, dest_path)

            # Clean up temporary directory
            shutil.rmtree(temp_dir)

        except Exception as e:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise RuntimeError(f"Failed to extract {archive_path}. Error: {e}")

    def _is_archive(self, file_path):
        return DownloadDataset._is_archive(self, file_path)


class ProcessTextFiles(luigi.Task):
    """Process text files into dataframes."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def requires(self):
        return ExtractArchive(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(f"dataset/{self.dataset_name}_RAW_extracted_and_processed")

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
        # Get the directory where the original file is located
        base_name = os.path.splitext(original_filename)[0]
        file_dir = os.path.join(self.output().path, base_name)

        for key, df in dfs.items():
            # Special handling for Probes dataframe
            if key == 'Probes':
                # Save full version
                df.to_csv(
                    os.path.join(file_dir, f"{base_name}_Probes_full.csv"),
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
                    os.path.join(file_dir, f"{base_name}_Probes_short.csv"),
                    index=False
                )
            else:
                df.to_csv(
                    os.path.join(file_dir, f"{base_name}_{key}.csv"),
                    index=False
                )


class GEOPipeline(luigi.WrapperTask):
    """Main pipeline task that processes a GEO dataset."""

    dataset_name = luigi.Parameter(description="Dataset accession number")

    def requires(self):
        return ProcessTextFiles(dataset_name=self.dataset_name)


if __name__ == '__main__':
    luigi.run()
