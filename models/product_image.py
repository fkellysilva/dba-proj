from datetime import datetime
from bson import ObjectId

class ImagemProduto:
    def __init__(self, codigo_produto, url_imagem, descricao=None, imagem_principal=False):
        self._id = ObjectId()
        self.codigo_produto = codigo_produto
        self.url_imagem = url_imagem
        self.descricao = descricao
        self.imagem_principal = imagem_principal
        self.data_criacao = datetime.now()
        self.data_atualizacao = datetime.now()

    def to_dict(self):
        return {
            '_id': str(self._id),
            'codigo_produto': self.codigo_produto,
            'url_imagem': self.url_imagem,
            'descricao': self.descricao,
            'imagem_principal': self.imagem_principal,
            'data_criacao': self.data_criacao.isoformat(),
            'data_atualizacao': self.data_atualizacao.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        imagem = cls(
            codigo_produto=data['codigo_produto'],
            url_imagem=data['url_imagem'],
            descricao=data.get('descricao'),
            imagem_principal=data.get('imagem_principal', False)
        )
        imagem._id = ObjectId(data['_id'])
        imagem.data_criacao = datetime.fromisoformat(data['data_criacao'])
        imagem.data_atualizacao = datetime.fromisoformat(data['data_atualizacao'])
        return imagem 