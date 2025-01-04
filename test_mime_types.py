import magic


def get_mime_type(file_path):
    mime = magic.Magic(mime=True)
    return mime.from_file(file_path)


# Our current list
our_mime_types = [
    'application/zip',
    'application/x-tar',
    'application/x-gzip',
    'application/gzip',
    'application/x-bzip2',
    'application/x-rar-compressed',
    'application/x-7z-compressed',
    'application/x-xz',
    'application/x-lzip',
    'application/x-lzma',
    'application/x-zstd',
    'application/x-cpio',
    'application/x-archive',
    'application/x-shar',
]

print("Current MIME type for .gz file:")
print(get_mime_type('dataset_GSE68849/GPL10558_HumanHT-12_V4_0_R1_15002873_B.txt.gz'))

print("\nOur supported MIME types:")
for mime in sorted(our_mime_types):
    print(f"- {mime}")

print("\nNote: To get a complete list of actual MIME types, we should create test files")
print("of each archive type and check their MIME types. Common variations include:")
print("- application/gzip instead of application/x-gzip")
print("- application/x-bzip2 vs application/bzip2")
print("- application/x-xz vs application/xz")
print("- application/x-lzma vs application/lzma")
