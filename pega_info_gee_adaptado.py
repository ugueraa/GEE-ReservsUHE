# Desenvolvido por Ugo Maranhão Leal

import ee
import geemap
import pandas as pd
import geopandas as gpd
import os
from datetime import datetime as dt
from tqdm import tqdm
import numpy as np

# For the first time running the code, uncoment the line below
#ee.Authenticate()

ee.Initialize()

def img_ids(imgcollection):
    ids = [item.get('id') for item in imgcollection.getInfo().get('features')]
    return ids

def img_datetime(img):
    imgdate = dt.fromtimestamp(img.get("system:time_start").getInfo() / 1000.)
    return imgdate


def save_timeserie(pixels_timeseries, path_final):
	# satelite name pode ser um nome para o arquivo excel
	# pixel_timeserires pode ser um dicionario qualquer
		writer = pd.ExcelWriter(path_final, engine='xlsxwriter')
		
		for ide in pixels_timeseries:
			pixels_timeseries[ide].to_excel(writer, sheet_name=ide, index=True)
		
		writer.save()

def pixels_values(imgcollection, geometries, list_band_name, id_name): # devolve dicionário com um dataframe por banda
	
    ids_list = img_ids(imgcollection)

    pixel_all_values = []
    dic = {}
    dates = []
    

    for band_name in list_band_name:
        pixel_all_values = []
        for id in tqdm(ids_list):
            image = ee.Image(id).select(band_name)
				
            image_pixels = image.reduceRegions(
                reducer=ee.Reducer.mean(),
                collection=geometries,
                # scale=9999,
            )

    
            img_date = img_datetime(image)
            values = []
            for pixel in image_pixels.getInfo()['features']:
                geom = pixel['geometry']['coordinates']
                pixel_id = pixel['properties'][id_name]
    
                try:
                    mean = pixel['properties']['mean']
                except:
                    mean = np.nan
    
                values.append([mean, geom, pixel_id])
            
            dateing = dt(img_date.year,img_date.month,img_date.day,img_date.hour,img_date.minute)
            pixel_all_values.append([dateing, values])

        dfs = [pd.DataFrame(pt[1], columns=[pt[0], "geom", "id"]) for pt in pixel_all_values]
        geom,id_pontos = dfs[0]['geom'].tolist(),dfs[0]['id'].tolist()
        
        for i,id_p in enumerate(id_pontos): id_pontos[i] = "ID-"+str(id_p)  

        df_concat       = pd.concat(dfs, axis=1, join="inner")
        df_concat       = df_concat.drop(["geom", "id"], axis=1)
        df_concat['ID'] = id_pontos
        df_concat       = df_concat.set_index('ID').T
        df_concat.index.name = "Data"
        df_concat.index = pd.to_datetime(df_concat.index)
        dic[band_name]  = df_concat
    
    return dic
    
def atualizar_dados(satelite,bandas,begin,end,shape):
	
	nome_final = satelite.split('/')[0]+str(dt.now()).split('.')[0].replace(' ','-').replace(':','-')+'.xlsx'
	
	print('RODANDO PARA ',satelite)
	
	# Abrir shapefile com geopandas visualização
	dt_geom           = gpd.read_file(shape)
	dt_geom["Codigo"] = dt_geom["ID"]
	dt_geom           = dt_geom[["Codigo", "geometry"]]
	
	print('GEOMETRIA SHAPE ENTRADA: ',dt_geom['geometry'])
	
	#import do shapefile utilizado para os pontos em EE
	points_shp = geemap.shp_to_ee((shape))
	
	# Landsat 7 SR
	
	if satelite == 'Landsat7':
		dataset = ee.ImageCollection('LANDSAT/LE07/C02/T1_L2') \
			.select(bandas) \
			.filterMetadata('CLOUD_COVER', 'less_than', 20) \
			.filterBounds(points_shp) \
			.filterDate(begin, end) \
			.sort('system:time_start')
	
	
	elif satelite == 'Sentinel2':
		dataset = ee.ImageCollection('COPERNICUS/S2_SR') \
			.select(bandas) \
			.filterBounds(points_shp) \
			.filterDate(begin, end) \
			.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)) \
			.sort('system:time_start') 
	
	else:
		dataset = ee.ImageCollection(satelite) \
			.select(bandas) \
			.filterBounds(points_shp) \
			.filterDate(begin, end) \
			.sort('system:time_start') 
		
		
	dataset = dataset.map(lambda img: img.clip(points_shp))
	
	
	# transformando colecao de imagens em um dataframe 
	pixels_timeseries = []
	pixels_timeseries = pixels_values(dataset, points_shp, bandas, id_name='ID')
	
	path_final = os.getcwd() + '/saida/' + nome_final
	save_timeserie(pixels_timeseries,path_final)
	
	#print('finalizado para: ' + metodo+' '+reservatorio)
	
		
	print("FINALIZADO!! todos dados foram gerados p/ Satelite = "+satelite+ ' No periodo de '+begin+' até '+end+' !!!! Confira a pasta finais em seu diretório raiz')

if __name__ == '__main__':
	# Código que produz série histórica dos valores dos pixels georreferenciados (lat,long) para bandas especificadas 

	'''
	Este programa esta configurado para retirar dados de duas fontes de satélite - aplicavel filtro de nebulosidade
	* Sentinel2
	* LandSat7
	
	Caso seja desejado o uso de outro conjunto de imagens de satélite basta inserir o caminho da ImageCollection('caminho') 
	na variável satelite. Ex:'COPERNICUS/S2_SR'
	'''
	
	### Definicoes para rodar aquivo
	satelite       = 'Landsat7'                  # satélite
	bandas         = ['ST_B6','ST_DRAD']         # lista de bandas  
	begin          = f"2018-01-01"               # inicio intervalo tempo
	end            = f"2018-05-31"               # final intervalo tempo
	shape          = '/media/uguera/Bckps-HD/IC_integralizaçao/bckp/TCC_2/bckp/Pontos_Calibracao/Juru_ponto_calibracao.shp'                # shapefile WGS84 pontos de extração de dados 
	                             
	if 'saida' not in os.listdir(): os.mkdir(os.getcwd() + '/saida')
	dir_save       = 'saida/'
	
	
	atualizar_dados(satelite,bandas,begin,end,shape)












































