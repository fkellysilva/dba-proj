from datetime import datetime
from bson import ObjectId

class Comentario:
    def __init__(self, codigo_produto, id_cliente, texto, avaliacao=None, imagens=None):
        self._id = ObjectId()
        self.codigo_produto = codigo_produto
        self.id_cliente = id_cliente
        self.texto = texto
        self.avaliacao = avaliacao
        self.imagens = imagens or []
        self.data_criacao = datetime.now()
        self.data_atualizacao = datetime.now()

    def to_dict(self):
        return {
            '_id': str(self._id),
            'codigo_produto': self.codigo_produto,
            'id_cliente': self.id_cliente,
            'texto': self.texto,
            'avaliacao': self.avaliacao,
            'imagens': self.imagens,
            'data_criacao': self.data_criacao.isoformat(),
            'data_atualizacao': self.data_atualizacao.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        comentario = cls(
            codigo_produto=data['codigo_produto'],
            id_cliente=data['id_cliente'],
            texto=data['texto'],
            avaliacao=data.get('avaliacao'),
            imagens=data.get('imagens', [])
        )
        comentario._id = ObjectId(data['_id'])
        comentario.data_criacao = datetime.fromisoformat(data['data_criacao'])
        comentario.data_atualizacao = datetime.fromisoformat(data['data_atualizacao'])
        return comentario 