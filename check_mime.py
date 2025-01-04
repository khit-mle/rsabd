import magic

file_path = 'dataset_GSE68849/GPL10558_HumanHT-12_V4_0_R1_15002873_B.txt.gz'
mime = magic.Magic(mime=True)
print(f"MIME type: {mime.from_file(file_path)}")
