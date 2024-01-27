from math import remainder
from operator import methodcaller
import os
import gzip
import logging
import pathlib
from dataclasses import dataclass
from socketserver import StreamRequestHandler
import typing as t
import click
import socket

import shutil

from stat import *

from http_messages import HTTPRequest, HTTPResponse, APPLICATION_OCTET_STREAM, BASE_TEXT, TEXT_PLAIN, GET, POST, PUT, DELETE

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class HTTPServer:
    server_address: t.Tuple[str, int]
    socket: socket.socket
    server_domain: str
    working_directory: pathlib.Path


class HTTPHandler(StreamRequestHandler):
    server: HTTPServer

    # Use self.rfile and self.wfile to interact with the client
    # Access domain and working directory with self.server.{attr}
    def handle(self) -> None:
        # TODO: Write your code
        first_line = self.rfile.readline()
        logger.info(f"Handle connection from {self.client_address}, first_line {first_line}")
        request = HTTPRequest.from_bytes(first_line)
        rn_cnt = 0
        while line := self.rfile.readline():
            line = line.decode()
            splitted_line = line.split()
            logger.debug(f"{splitted_line=}")
            if line == "\r\n":
                rn_cnt += 1
            else:
                request.insert_header(splitted_line[0][:-1], splitted_line[1])
                rn_cnt = 0
            if rn_cnt == 1:
                break

        path_to_file = self.server.working_directory.absolute().as_posix() + (first_line.decode().split()[1])
        d = {}
        d.update({"Server": "server"})
        block_sz = 64 * (10**6)
        content = ""
        if "Host" in request.headers.keys():
            logger.debug(f"{self.server.server_domain=} {request.headers['Host']=}")
        if ("Host" in request.headers.keys()) and (self.server.server_domain != "" and self.server.server_domain != request.headers["Host"]):
            if "Content-Length" in request.headers.keys():
                new_data = self.rfile.read(int(request.headers["Content-Length"]))
            logger.debug(f"HOST BAD_REQUEST: {self.server.server_domain=} {request.headers['Host']=}")
            response = HTTPResponse(request.version, "BAD_REQUEST", d)
            d.update({"Content-Length": str(4)})
            d.update({"Content-Type": BASE_TEXT})
            content = b"Bad\n"
        elif request.method == GET:
            if os.path.exists(path_to_file):
                logger.debug("GET file exists.")
                response = HTTPResponse(request.version, "OK", d)
                mode = os.lstat(path_to_file).st_mode
                if path_to_file[-1] == "/":
                    pass
                elif S_ISDIR(mode):
                    logger.debug("GET dir=True")
                    content = str(os.popen(f"ls -lA --time-style=+%Y-%m-%d %H:%M:%S {path_to_file}").read())
                    file_sz = len(str(content))
                    d.update({"Content-Length": str(file_sz)})
                    d.update({"Content-Type": TEXT_PLAIN})

                    content = content.encode()
                    logger.debug(f"{content=}")
                else:
                    file_sz = os.stat(path_to_file).st_size
                    d.update({"Content-Length": str(file_sz)})
                    d.update({"Content-Type": BASE_TEXT})

                    logger.debug(f"{file_sz=} {path_to_file=}")
                    file = open(path_to_file, "rb")
                    if "Accept-Encoding" in request.headers.keys() and request.headers["Accept-Encoding"] == "gzip":
                        logger.debug("compressed")
                        d.update({"Content-Encoding": "gzip"})
                        logger.debug(d)
                        response = HTTPResponse(request.version, "OK", d)
                        remained_sz = os.path.getsize(path_to_file)
                        tmp_path = pathlib.Path(path_to_file).parent.absolute().as_posix() + "/my_tmp.gz"
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                        logger.debug(f"{tmp_path=}")

                        fout = gzip.open(tmp_path, "wb")
                        logger.debug("starting write to gzip")
                        while remained_sz > 0:
                            read_sz = block_sz if remained_sz >= block_sz else remained_sz
                            logger.debug(f"{read_sz=}")
                            content = file.read(read_sz)
                            remained_sz -= read_sz
                            fout.write(content)
                        fout.close()

                        remained_sz = os.path.getsize(tmp_path)
                        d["Content-Length"] = str(remained_sz)
                        self.wfile.write(response.to_bytes())
                        fin = open(tmp_path, "rb")
                        logger.debug(f"starting read from gzip, {remained_sz=}")
                        while remained_sz > 0:
                            read_sz = block_sz if remained_sz >= block_sz else remained_sz
                            logger.debug(f"{read_sz=}")
                            gzip_content = fin.read(read_sz)
                            remained_sz -= read_sz
                            self.wfile.write(gzip_content)
                        fin.close()
                        os.remove(tmp_path)
                    else:
                        logger.debug("not compressed")
                        self.wfile.write(response.to_bytes())
                        remained_sz = os.path.getsize(path_to_file)
                        logger.debug("start")
                        while remained_sz > 0:
                            read_sz = block_sz if remained_sz >= block_sz else remained_sz
                            logger.debug(f"{read_sz=}")
                            content = file.read(read_sz)
                            remained_sz -= read_sz
                            self.wfile.write(content)
                    return
            else:
                logger.debug("GET file does not exist.")
                response = HTTPResponse(request.version, "NOT_FOUND", d)
                d.update({"Content-Length": str(4)})
                content = b"Bad\n"

        elif request.method == POST:
            remained_sz = 0
            if "Content-Length" in request.headers.keys():
                remained_sz = int(request.headers["Content-Length"])
            logger.debug(f"{remained_sz=}")
            if os.path.exists(path_to_file):
                logger.debug("POST CONFLICT")
                response = HTTPResponse(request.version, "CONFLICT", d)
                d.update({"Content-Length": str(4)})
                d.update({"Content-Type": BASE_TEXT})
                content = b"Bad\n"
            elif "Create-Directory" in request.headers.keys() and request.headers["Create-Directory"] == "True":
                logger.debug(f"POST OK Create-Directory")
                response = HTTPResponse(request.version, "OK", d)
                d.update({"Content-Length": str(4)})
                d.update({"Content-Type": BASE_TEXT})
                content = b"OK.\n"
                pathlib.Path(path_to_file).parent.mkdir(exist_ok=True, parents=True)
                os.mkdir(path_to_file)
            else:
                logger.debug(f"POST OK")
                response = HTTPResponse(request.version, "OK", d)
                d.update({"Content-Length": str(4)})
                d.update({"Content-Type": BASE_TEXT})
                content = b"OK.\n"
                pathlib.Path(path_to_file).parent.mkdir(exist_ok=True, parents=True)
                file = open(path_to_file, "wb")
                logger.debug("start")
                while remained_sz > 0:
                    read_sz = block_sz if remained_sz >= block_sz else remained_sz
                    logger.debug(f"{read_sz=}")
                    body = self.rfile.read(read_sz)
                    remained_sz -= read_sz
                    file.write(body)
                file.close()
            while remained_sz > 0:
                read_sz = block_sz if remained_sz >= block_sz else remained_sz
                _ = self.rfile.read(read_sz)
                remained_sz -= read_sz

        elif request.method == PUT:
            remained_sz = 0
            if "Content-Length" in request.headers.keys():
                remained_sz = int(request.headers["Content-Length"])
            logger.debug(f"{remained_sz=}")

            if os.path.exists(path_to_file) == False:
                logger.debug("PUT CONFLICT does not exist")
                response = HTTPResponse(request.version, "CONFLICT", d)
                d.update({"Content-Length": str(4)})
                d.update({"Content-Type": BASE_TEXT})
                content = b"Bad\n"
            else:
                mode = os.lstat(path_to_file).st_mode
                if S_ISDIR(mode):
                    logger.debug("PUT CONFLICT dir")
                    response = HTTPResponse(request.version, "CONFLICT", d)
                    d.update({"Content-Length": str(4)})
                    d.update({"Content-Type": BASE_TEXT})
                    content = b"Bad\n"
                else:
                    logger.debug("PUT UPDATE")
                    response = HTTPResponse(request.version, "OK", d)
                    d.update({"Content-Length": str(4)})
                    d.update({"Content-Type": BASE_TEXT})
                    content = b"OK.\n"
                    file = open(path_to_file, "wb")
                    while remained_sz > 0:
                        read_sz = block_sz if remained_sz >= block_sz else remained_sz
                        logger.debug(f"{read_sz=}")
                        body = self.rfile.read(read_sz)
                        remained_sz -= read_sz
                        file.write(body)
                    file.close()
            while remained_sz > 0:
                read_sz = block_sz if remained_sz >= block_sz else remained_sz
                _ = self.rfile.read(read_sz)
                remained_sz -= read_sz

        elif request.method == DELETE:
            if "Content-Length" in request.headers.keys():
                new_data = self.rfile.read(int(request.headers["Content-Length"]))
            if os.path.exists(path_to_file) == False:
                logger.debug("DELETE CONFLICT")
                response = HTTPResponse(request.version, "CONFLICT", d)
                d.update({"Content-Length": str(4)})
                d.update({"Content-Type": BASE_TEXT})
                content = b"Bad\n"
            else:
                mode = os.lstat(path_to_file).st_mode
                if S_ISDIR(mode):
                    if "Remove-Directory" in request.headers.keys():
                        logger.debug("DELETE OK Remove-Directory")
                        response = HTTPResponse(request.version, "OK", d)
                        d.update({"Content-Length": str(4)})
                        d.update({"Content-Type": BASE_TEXT})
                        content = b"OK.\n"
                        shutil.rmtree(pathlib.Path(path_to_file))
                    else:
                        logger.debug("DELETE NOT_ACCEPTABLE")
                        response = HTTPResponse(request.version, "NOT_ACCEPTABLE", d)
                        d.update({"Content-Length": str(4)})
                        d.update({"Content-Type": BASE_TEXT})
                        content = b"BAD\n"
                else:
                    response = HTTPResponse(request.version, "OK", d)
                    d.update({"Content-Length": str(4)})
                    d.update({"Content-Type": BASE_TEXT})
                    content = b"OK.\n"
                    os.remove(path_to_file)
        logger.debug("done.")
        self.wfile.write(response.to_bytes())
        self.wfile.write(content)


@click.command()
@click.option("--host", type=str)
@click.option("--port", type=int)
@click.option("--server-domain", type=str)
@click.option("--working-directory", type=str)
def main(host, port, server_domain, working_directory):
    # TODO: Write your code
    if host is None:
        if "SERVER_HOST" in os.environ:
            host = os.environ.get("SERVER_HOST")
        else:
            host = "0.0.0.0"
    if port is None:
        if "SERVER_PORT" in os.environ:
            port = os.environ.get("SERVER_PORT")
        else:
            port = 8080
    if server_domain is None:
        if "SERVER_DOMAIN" in os.environ:
            server_domain = os.environ.get("SERVER_DOMAIN")
        else:
            server_domain = ""
    if working_directory is None:
        if "SERVER_WORKING_DIRECTORY" in os.environ:
            working_directory = os.environ.get("SERVER_WORKING_DIRECTORY")
        else:
            working_directory = ""
    working_directory_path = pathlib.Path(working_directory)
    if working_directory == "":
        exit(1)

    logger.info(f"Starting server on {host}:{port}, domain {server_domain}, working directory {working_directory}")

    # Create a server socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Set SO_REUSEADDR option
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind the socket object to the address and port
    s.bind((host, port))
    # Start listening for incoming connections
    s.listen()

    logger.info(f"Listening at {s.getsockname()}")
    server = HTTPServer((host, port), s, server_domain, working_directory_path)

    while True:
        # Accept any new connection (request, client_address)
        try:
            conn, addr = s.accept()
        except OSError:
            break

        try:
            # Handle the request
            HTTPHandler(conn, addr, server)

            # Close the connection
            conn.shutdown(socket.SHUT_WR)
            conn.close()
        except Exception as e:
            logger.error(e)
            conn.close()


if __name__ == "__main__":
    main(auto_envvar_prefix="SERVER")
