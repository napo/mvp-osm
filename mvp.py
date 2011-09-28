# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 12:01:27 2011

@author: Maurizio Napolitano
MIT License
"""

from pyspatialite import dbapi2 as db
from datetime import datetime
from datetime import timedelta
import scipy as sc
import numpy as np
import scipy.spatial.distance
import scipy.cluster.hierarchy
import ConfigParser
from optparse import OptionParser
import sys, os
TEMPLATECONFIG = "example.cfg"
class MVP():
    tables = ["osm_nodes"]
    sql_distinct = "select distinct(user) from "
    sql_count = "select count(distinct(user)) from "
    indb = None
    outdb = None
    days = None
    grid = None
    epsg =None
    goodtags = None    

    def __init__(self,indb,outdb,days,grid,epsg,goodtags):
        self.indb = indb
        self.outdb = outdb
        self.days = int(days)
        self.grid = grid
        self.epsg = epsg
        self.goodtags = goodtags
        
    def initdb(self):
        cur = db.connect(self.outdb)
        rs = cur.execute('SELECT sqlite_version(), spatialite_version()')
        for row in rs:
            msg = "> SQLite v%s Spatialite v%s" % (row[0], row[1])
        print msg
        sql= 'SELECT InitSpatialMetadata()'
        cur.execute(sql)
        sql = '''CREATE TABLE points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user STRING,
                timestamp INTEGER);'''
        cur.execute(sql)

        sql = '''SELECT AddGeometryColumn('points', 
                'geometry', %s, 'POINT', 'XY');''' % self.epsg
        cur.execute(sql)       

        sql = '''CREATE TABLE usersgrid (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                x FLOAT,
                y FLOAT,
                user STRING,
                density INTEGER,
                activity INTEGER,
                class INTEGER default 0);'''
	cur.execute(sql)
        
        sql = '''SELECT AddGeometryColumn('usersgrid',
                'geometry', %s, 'POINT', 'XY');''' % self.epsg
        cur.execute(sql)     
        
        sql = '''CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user STRING );'''
        cur.execute(sql)        

        # creating a POLYGON table
        sql = '''CREATE TABLE grid (
            id INTEGER PRIMARY KEY AUTOINCREMENT)'''
            
        cur.execute(sql)
        sql = '''SELECT AddGeometryColumn('grid',
             'geometry', %s, 'POLYGON', 'XY')''' % self.epsg
        cur.execute(sql)

        sql = "SELECT CreateSpatialIndex('points', 'geometry');"        
        cur.execute(sql)
        sql = "SELECT CreateSpatialIndex('usersgrid', 'geometry');"        
        cur.execute(sql)
        sql = "SELECT CreateSpatialIndex('grid', 'geometry');"        
        cur.execute(sql)
        
        sql = '''CREATE VIEW users_activity AS SELECT user,
                    (Round(JulianDay(max(timestamp)))
                    -(JulianDay(min(timestamp)))) as activity 
                    FROM points GROUP BY user;'''
        cur.execute(sql)
        
        sql = '''CREATE TABLE petlocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid INTEGER,
            user STRING,
            density INTEGER,
            activity INTEGER,
            class INTEGER default 0);'''
        cur.execute(sql)
        
        sql = '''SELECT AddGeometryColumn('petlocations',
                'geometry', %s, 'POLYGON', 'XY');''' % self.epsg
        cur.execute(sql) 

        sql = "SELECT CreateSpatialIndex('petlocations', 'geometry');"        
        cur.execute(sql)
        
        rs.close()
        cur.close()
        print "Init completed"
        
        
    def importusers(self):
        delta_days = self.days
        indb = db.connect(self.indb)
        dbout = db.connect(self.outdb)
        incur = indb.cursor()
        ago = ""
        if (delta_days == 0):
            ago = datetime.today() - timedelta(delta_days)
        else:
            sql = '''CREATE VIEW users_lastdays as SELECT user,
            MAX(timestamp) as tempo FROM osm_nodes GROUP BY user;'''
            incur.execute(sql)
        
        s = 0
        for i in self.tables:
            
            if (delta_days > 0):
                sql = '''select distinct(user) from 
                        users_lastdays where tempo > "%s"''' % str(ago)
            else:
                sql = "SELECT distinct(user) from osm_nodes";
                
            rs = incur.execute(sql)
            r = rs.fetchall()
            if s == 0:
                outcur = dbout.cursor()
                for u in r:
                    user = u[0]
                    sql = "INSERT INTO users (user) VALUES ('%s')" % (user)
                    outcur.execute(sql)
                s = s+1
                outcur.close()
                dbout.commit()
            if (delta_days >0):
                sql = "DROP VIEW users_lastdays;"
                incur.execute(sql)

            else:
                outcur = dbout.cursor()
                for u in r:
                    user = u[0]
                    sql = "Select user from users where user = '%s';" % user
                    rsu = list(outcur.execute(sql))
                    if len(rsu) == 0:
                        sql = "INSERT INTO users (user) VALUES ('%s')" % (user)
                        outcur.execute(sql)
                outcur.close()
                dbout.commit()
        incur.close()
        indb.close()
        dbout.close()
        print "Users imported"
    
    def insertptlnodes(self):
        print "search nodes"
        indb = db.connect(self.indb)  
        incur = indb.cursor()
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor()
        for table in self.tables:  
            if table == 'osm_nodes':
                w =' in ('
                for t in self.goodtags:
                    t = "'" + t.rstrip() + "',"
                    w += t
                w += ")"
                where_badtags = w.replace("(,","(")
                w = where_badtags.replace(",)",")") 
                
                sql = 'select X(transform(osm_nodes.Geometry,%s)) as x,' % (self.epsg)
                sql += 'Y(transform(osm_nodes.Geometry,%s)) as y ' % (self.epsg)
                sql += ', timestamp, user from osm_nodes '
                sql += ' natural join %s_tags where %s_tags.k' % (table.rstrip('s'),table.rstrip('s'))                
                sql += w                   
                sql == " GROUP BY user;"
                rs = incur.execute(sql)
                for r in rs:
                    if (r[2] != None):
                        p = "GeomFromText('POINT(%s %s)',%s)"  % (r[0],r[1],self.epsg)
                        sql = "INSERT INTO points (user, timestamp, geometry) "
                        sql += "VALUES ('%s','%s',%s)" % (r[3],r[2],p)               
                        outcur.execute(sql)
                dbout.commit()
            else:
                #FIX!!!
                for t in self.goodtags:
                    idname = table.replace("osm_","").rstrip('s') + "_id"
                    idname = idname.replace("relation","rel")
                    sql = 'select distinct(%s.%s) from %s' % (table,idname, table)
                    sql += ' natural join %s_tags where ' % (table.rstrip('s'))
                    sql += '%s_tags.k like "%s" ' % (table.rstrip('s'),t.rstrip())
                    sql += " group bu user"
                    rs = incur.execute(sql)
                    ids = rs.fetchall()
                    for i in ids:
                        sql = 'select distinct(osm_nodes.node_id), timestamp from osm_nodes natural join %s_node_refs where '  % (table.rstrip('s'))
                        sql += '%s_node_refs.%s = %s' % (table.rstrip('s'),idname,i[0])
                        rs = incur.execute(sql)
                        idp = rs.fetchall()
                        for ip in idp:
                            sql = 'select X(transform(osm_nodes.Geometry,%s)) as x,' % (self.epsg)
                            sql += 'Y(transform(osm_nodes.Geometry,%s)) as y, osm_nodes.timestamp '  % (self.epsg) 
                            sql += ' from osm_nodes'  
                            sql += ' where osm_nodes.node_id = %s' % ip[0]
                            record = incur.execute(sql)
                            v = record.fetchone()
                            p = "GeomFromText('POINT(%s %s)',%s)"  % (v[0],v[1],self.epsg)
                            sql = "INSERT INTO points (user, timestamp,age, geometry) "
                            sql += "VALUES ('%s','%s', %d,%s)" % ("", ip[1], -1,p) 
                            outcur.execute(sql)
                            dbout.commit()
        outcur.close()
        dbout.close()
        incur.close()
        indb.close()
        
    def createusersgrid(self):
        print "Create users grid"
        indb = db.connect(self.indb)  
        incur = indb.cursor()
        sql = '''SELECT Min(ST_X(transform(osm_nodes.geometry,%s))) AS min_x, 
                        Min(ST_Y(transform(geometry,%s))) AS min_y, 
                        Max(ST_X(transform(geometry,%s))) AS max_x, 
                        Max(ST_Y(transform(geometry,%s))) AS max_y 
                        FROM osm_nodes;'''  % (self.epsg,self.epsg,self.epsg,self.epsg) 
                        
        sql = '''SELECT Min(ST_X(osm_nodes.geometry)) AS min_x, 
                        Min(ST_Y(osm_nodes.geometry)) AS min_y, 
                        Max(ST_X(osm_nodes.geometry)) AS max_x, 
                        Max(ST_Y(osm_nodes.geometry)) AS max_y 
                        FROM osm_nodes;'''  
        rs = incur.execute(sql).fetchone()
        minx = rs[0]
        miny = rs[1]
        maxx = rs[2]
        maxy = rs[3]
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor()
        sql = 'SELECT ST_X(transform(MakePoint(%s,%s,4326),%s)),' % (minx,miny,self.epsg)
        sql += 'ST_Y(transform(MakePoint(%s,%s,4326),%s)),' % (minx,miny,self.epsg)
        sql += 'ST_X(transform(MakePoint(%s,%s,4326),%s)),' % (maxx,maxy,self.epsg)
        sql += 'ST_Y(transform(MakePoint(%s,%s,4326),%s))''' % (maxx,maxy,self.epsg)  
        rs = outcur.execute(sql).fetchone()
        minx = rs[0]
        miny = rs[1]
        maxx = rs[2]
        maxy = rs[3]
        stepminx = minx
        stepmaxx = minx + self.grid
        stepminy = miny
        stepmaxy = miny + self.grid

        while(True):  
 
            sql =  '''select count(id),
			    (Round(JulianDay(max(timestamp))) - 
			    Round(JulianDay(min(timestamp)))) as activity,
                            user 
                            from points where points.ROWID in
                            (select pkid from idx_points_geometry 
                            where pkid match 
                            RTreeIntersects(%d, %d, %d, %d)) and points.user in (
                            select user from users_activity where activity > %i);'''  % (stepminx,stepminy,stepmaxx,stepmaxy,self.days)                  

            rs = outcur.execute(sql).fetchone()

            x = stepminx + float(self.grid/2)
            y = stepminy + float(self.grid/2)

            if rs != None:            
                density = rs[0]
                activity = rs[1]
                if activity == None:
                    activity = 0
                user = rs[2]
                if user != None:
                    p = "GeomFromText('POINT(%s %s)',%s)"  % (x,y,self.epsg)
                    sql = "INSERT INTO usersgrid (x, y,user, density,activity,geometry) "
                    sql += "VALUES (%f, %f,'%s',%i,%i,%s)" % (float(x),float(y),user,density,activity,p)
                    outcur.execute(sql)
                    dbout.commit()

            if (stepmaxx <= maxx):
                stepminx = stepmaxx
                stepmaxx += self.grid
            else:
                stepminx = minx 
                stepmaxx = minx + self.grid
                stepminy += self.grid
                stepmaxy += self.grid
 
                
                if (stepmaxy >= maxy):
                    break

        incur.close()
        indb.close()
        
    def creategrid(self,res):
        print "Create grid"
        indb = db.connect(self.indb)  
        incur = indb.cursor()
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor()
        sql = '''SELECT Min(ST_X(transform(osm_nodes.geometry,%s))) AS min_x, 
                        Min(ST_Y(transform(geometry,%s))) AS min_y, 
                        Max(ST_X(transform(geometry,%s))) AS max_x, 
                        Max(ST_Y(transform(geometry,%s))) AS max_y 
                        FROM osm_nodes;'''  % (self.epsg,self.epsg,self.epsg,self.epsg) 
                        
        sql = '''SELECT Min(ST_X(osm_nodes.geometry)) AS min_x, 
                        Min(ST_Y(osm_nodes.geometry)) AS min_y, 
                        Max(ST_X(osm_nodes.geometry)) AS max_x, 
                        Max(ST_Y(osm_nodes.geometry)) AS max_y 
                        FROM osm_nodes;'''  
        rs = incur.execute(sql).fetchone()
        minx = rs[0]
        miny = rs[1]
        maxx = rs[2]
        maxy = rs[3]
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor()
        sql = 'SELECT ST_X(transform(MakePoint(%s,%s,4326),%s)),' % (minx,miny,self.epsg)
        sql += 'ST_Y(transform(MakePoint(%s,%s,4326),%s)),' % (minx,miny,self.epsg)
        sql += 'ST_X(transform(MakePoint(%s,%s,4326),%s)),' % (maxx,maxy,self.epsg)
        sql += 'ST_Y(transform(MakePoint(%s,%s,4326),%s))''' % (maxx,maxy,self.epsg)  
        rs = outcur.execute(sql).fetchone()
        minx = rs[0]
        miny = rs[1]
        maxx = rs[2]
        maxy = rs[3]
        stepminx = minx
        stepmaxx = minx + res
        stepminy = miny
        stepmaxy = miny + res       
        
        while(True):
            p = "GeomFromText('POLYGON(("
            p += "%f %f, " % (stepminx, stepminy)
            p += "%f %f, " % (stepmaxx, stepminy)
            p += "%f %f, " % (stepmaxx, stepmaxy)
            p += "%f %f, " % (stepminx, stepmaxy)
            p += "%f %f" % (stepminx, stepminy)
            p += "))',%s)" % self.epsg
            sql = "INSERT INTO grid (geometry) "
            sql += "VALUES (%s);" % p
            outcur.execute(sql)
            if (stepmaxx <= maxx):
                stepminx = stepmaxx
                stepmaxx += res
            else:
                stepminx = minx 
                stepmaxx = minx + res
                stepminy += res
                stepmaxy += res
                
                if (stepmaxy >= maxy):
                    break
        dbout.commit()
        outcur.close()
        
    def clustergridgroup(self,gridsize):
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor() 
        print "Calculate cluster ..."
        users = self.getusers()
        for u in users:
            sql = "SELECT id,x,y FROM usersgrid WHERE user='%s'" % u;
            rs = outcur.execute(sql)        
            result = []
            ids = []
            for r in rs:
                t = (r[1],r[2])
                result.append(t)
                ids.append(r[0])
            if len(result) > 1:
                d = np.array(result)
                dist = scipy.spatial.distance.pdist(d, 'euclidean')
                Z = sc.cluster.hierarchy.single(dist)
                clustgroup = sc.cluster.hierarchy.fcluster(Z, t=gridsize, criterion='distance')
                k = 0
                out = dbout.cursor() 
                for c in clustgroup:
                    c = int(c)
                    idp = int(ids[k])
                    sql = '''UPDATE usersgrid
                            SET class=%i
                            WHERE id=%i;''' % (c,idp)
                    out.execute(sql)  
                    dbout.commit()
                    k +=1                
                out.close()
                    
        outcur.close();
        
    def getusers_activity(self,maxactivity):
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor() 
        sql = "select user from users_activity where activity > %i;" % maxactivity
        users = list(outcur.execute(sql))
        outcur.close()
        return users        

    def getusers(self):
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor() 
        sql = "SELECT user FROM users"
        users = list(outcur.execute(sql))
        outcur.close()
        return users

    def petlocations(self):
        print "calculate petlocations"
        dbout = db.connect(self.outdb)
        outcur = dbout.cursor() 
        sql = '''select count(grid.id) as gid, asText(CastToPolygon(gunion(grid.geometry))) as geometry, 
                usersgrid.class as class, usersgrid.user as user, 
                max(usersgrid.activity) as activity, 
                max(usersgrid.density) as density,
                geometrytype(gunion(grid.geometry)) as tipo from usersgrid,
                grid where usersgrid.rowid in 
                (select pkid from idx_usersgrid_geometry 
                where pkid match 
                RTreeIntersects(MbrMinX(grid.geometry),
                                MbrMinY(grid.geometry), 
                                MbrMaxX(grid.geometry),
                                MbrMaxY(grid.geometry))) group 
                                by usersgrid.class, usersgrid.user order by user desc;'''
        rs = outcur.execute(sql).fetchall()
        for r in rs:
            gid = r[0]
            geometry = r[1]
            clas = r[2]
            user = r[3]
            activity = r[4]
            density = r[5]
            print r[6]
            sql = '''INSERT INTO petlocations (gid, geometry, class, user,activity,density)
                     VALUES (%s,GeomFromText('%s',%s),%s,'%s',%s,%s)''' % (gid, geometry, self.epsg,clas, user,activity,density)
            outcur.execute(sql)
            dbout.commit()
        outcur.close()

def execMVP(cmd):
    days = None
    epsg = None
    outdb = None
    indb = None
    grid = None
    goodtags = "conf/goodtags.txt"

    if (cmd.config):
        try:
            parser = ConfigParser.ConfigParser()
            parser.readfp(open(cmd.config));
            filename = parser.get("goodtags","file")
            f = open(filename,'r')
            goodtags = f.readlines()
            epsg = parser.get("config","epsg")
            days = parser.get("config","days")
            indb = parser.get("indb","infile")
            outdb = parser.get("outdb","outfile")
        except ConfigParser.NoOptionError, e:
            print "Error %s " % e
            sys.exit(2)    
            
    if (cmd.input):
        indb = cmd.input
    if (cmd.output):
        outdb = cmd.output
    if (cmd.tags):
        try:
            f = open(cmd.tags,'r')
            goodtags = f.readlines()
        except OSError, e:
            print "Error " +  e
            sys.exit(2)
    if (cmd.epsg):
        epsg = cmd.epsg
    if (cmd.grid):
        grid = cmd.grid
    if (cmd.days):
        days = cmd.days
    
    if days == None:
        days = 180
    if grid == None:
        grid = 1000
    if epsg == None:
        epsg = "epsg:900913"
        
    mu = MVP(indb,outdb,days,grid,epsg,goodtags)
    mu.initdb()
    mu.creategrid(grid)
    mu.importusers()
    mu.insertptlnodes()
    mu.createusersgrid()
    mu.clustergridgroup(grid)
    mu.petlocations();
    print "Enjoy your data on %s - i suggest to use qgis" %  cmd.output
        
def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)  
    parser = OptionParser(usage)
    parser.add_option("-c", "--config", action="store", dest="config", help="give a CONFIG file")
    parser.add_option("-C", "--create", action="store_true", dest="create", help="create a SAMPLE config file - usefull to create the config file",default=False)
    parser.add_option("-i", "--input", action="store", dest="input", help="input file *")
    parser.add_option("-o", "--output", action="store", dest="output", help="output file *")
    parser.add_option("-e", "--epsg", action="store", dest="epsg", help="metric epsg *")
    parser.add_option("-g","--grid",action="store",dest="grid",help="grid size expressed in epsg unit *")
    parser.add_option("-d", "--days", action="store", dest="days", help="users in the last N days *")    
    parser.add_option("-t", "--tags", action="store", dest="tags", help="txt file with the list of tags to search *")    
    (options,args) = parser.parse_args()
    if not options.create:
        if ((options.input) and (options.output) != None) or (options.config != None):
            execMVP(options)
        else:
            parser.print_help()
            print "* override the config file"
            sys.exit(0)            
    else:
        try:        
            f = open(os.getcwd() + os.path.sep + TEMPLATECONFIG, 'w')
            f.write("[config]\n")
            f.write("epsg: 900913\n")
            f.write("grid: 1000\n")
            f.write("days: 180\n")
            f.write("[goodtags]\n")
            f.write("file: conf/goodtags.txt\n")
            f.write("[indb]\n")
            f.write("infile:data/file.sqlite\n")
            f.write("[outdb]\n")
            f.write("outfile:data/mvposm.sqlite")
            f.close()
        except OSError, e:
            print "Error " + e
            sys.exit(2)
            
        
if __name__ == "__main__":
    main()
