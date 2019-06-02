/*Create Geometry for raster elements in a single tile.*/
/*
SELECT ST_AsText(ST_Transform(ST_Envelope(rast), 4326))
FROM t31tgn_20180925t104021_tci_10m;
*/

/*Try to save single patches for the given one.*/
SELECT write_file(ST_AsPNG(rast), '/tmp/slices/t31tgn_20180925t104021_tci_10m-p' || rid ||'.png')
FROM t31tgn_20180925t104021_tci_10m;


/*
Count the number of 120x120 tiles in the given table.
*/
/*
SELECT ST_AsText(ST_Transform(env, 4326))
FROM (
	SELECT ST_Width(rast.tiles) as width, ST_Envelope(rast.tiles) as env
	FROM (
		SELECT ST_Width(un) as width, ST_Tile(un, 120, 120) as tiles
		FROM (
			SELECT ST_Union(rast) as un FROM t31tgn_20180925t104021_tci_10m
		) as unioned
	) AS rast
) as tile
WHERE tile.width = 120;
*/
