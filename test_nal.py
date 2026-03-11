import zipfile
tmp = r'C:\Users\dhl\AppData\Local\Temp\florida_nal.zip'
zf = zipfile.ZipFile(tmp)
names = zf.namelist()
with zf.open(names[0]) as f:
    header = f.readline().decode('utf-8','replace').strip()
    cols = [c.strip('"') for c in header.split(',')]
    # Write to a file so we can see it all
    with open('nal_columns.txt', 'w') as out:
        for i, c in enumerate(cols):
            out.write(f'{i:3d}: {c}\n')
        out.write(f'\nTotal: {len(cols)} columns\n')
        spatial = [c for c in cols if any(k in c.upper() for k in ['LAT', 'LON', 'COORD', 'X_', 'Y_', 'CENT', 'GEO', 'DD'])]
        out.write(f'Spatial-ish: {spatial}\n')
zf.close()
print('Written to nal_columns.txt')
