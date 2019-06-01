/*
Get a list of all raster tables.
*/
DROP TABLE (
	SELECT r_table_name FROM raster_columns
);

/*
SELECT ST_AsText(ST_Envelope(rast))
FROM t31tgn_20180925t104021_tci_10m;
*/

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
