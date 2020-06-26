from flask import Blueprint, abort, jsonify, make_response, redirect
from flask import render_template_string, request, session, url_for

from passlib.context import CryptContext

from rating.api.queries import auth as query
from rating.api.queries import namespaces as namespace


auth_routes = Blueprint('authentication', __name__)
pwd_context = CryptContext(
    schemes=['pbkdf2_sha256'],
    default='pbkdf2_sha256',
    pbkdf2_sha256__default_rounds=30000
)


class UserAlreadyExist(Exception):
    pass


# Placeholder
def get_origin():
    return '*'
    # return 'http://localhost:8080'


def with_session(func):
    def wrapper(*args, **kwargs):
        kwargs['tenant'] = authenticated_user(request)
        res = func(**kwargs)
        total, results = res['total'], res['results']
        response = make_response(
            jsonify(results=results, total=total),
            200)
        response.headers['Access-Control-Allow-Origin'] = get_origin()
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        return response
    wrapper.__name__ = func.__name__
    return wrapper


def verify_user(tenant, password):
    results = query.get_tenant_id(tenant)[0]
    if check_encrypted_password(password, results['password']):
        return True
    return False


def encrypt_password(password):
    return pwd_context.encrypt(password)


def check_encrypted_password(password, hashed):
    return pwd_context.verify(password, hashed)


def authenticated_user(request):
    # Here default implicitly means public
    # e.g. namespaces not declared with tenant=whatever
    tenant = session.get('tenant')
    if tenant and query.get_tenant_id(tenant):
        return tenant
    return 'default'


@auth_routes.route('/login')
def login():
    login = """
        <div class="column is-4 is-offset-4">
            <h3 class="title">Login</h3>
            <div class="box">
                <form method="POST" action="/login_user">
                    <div class="field">
                        <div class="control">
                            <input
                            class="input is-large"
                            type="text"
                            name="tenant"
                            placeholder="Your account name"
                            autofocus="">
                        </div>
                    </div>

                    <div class="field">
                        <div class="control">
                            <input
                            class="input is-large"
                            type="password"
                            name="password"
                            placeholder="Your Password">
                        </div>
                    </div>
                    <div class="field">
                        <label class="checkbox">
                            <input type="checkbox">
                            Remember me
                        </label>
                    </div>
                    <button
                    class="button is-block is-info is-large is-fullwidth">Login</button>
                </form>
            </div>
        </div>
    """
    return render_template_string(login)


@auth_routes.route('/login_user', methods=['POST'])
def login_user():
    tenant = request.form.get('tenant')
    password = request.form.get('password')
    if verify_user(tenant, password):
        session['tenant'] = tenant
        return make_response(jsonify(message=f'{tenant} logged in'), 200)
    abort(make_response(jsonify(message='Invalid credentials'), 401))


@auth_routes.route('/signup')
def signup():
    signup = """
        <div class="column is-4 is-offset-4">
            <h3 class="title">Sign-up</h3>
            <div class="box">
                <form method="POST" action="/signup_user">
                    <div class="field">
                        <div class="control">
                            <input
                            class="input is-large"
                            type="text"
                            name="tenant"
                            placeholder="Your account name"
                            autofocus="">
                        </div>
                    </div>

                    <div class="field">
                        <div class="control">
                            <input
                            class="input is-large"
                            type="password"
                            name="password"
                            placeholder="Your Password">
                        </div>
                    </div>
                    <div class="field">
                        <div class="control">
                            <input
                            type="number"
                            id="quantity"
                            name="quantity"
                            value="1"
                            min="1"
                            max="5">
                        </div>
                    </div>
                    <button class="button is-block is-info is-large is-fullwidth">
                        Register
                    </button>
                </form>
            </div>
        </div>
    """
    return render_template_string(signup)


@auth_routes.route('/signup_user', methods=['POST'])
def signup_user():
    tenant = request.form.get('tenant')
    password = encrypt_password(request.form.get('password'))
    quantity = request.form.get('quantity', 1)
    if not query.get_tenant_id(tenant):
        query.new_tenant(tenant, password)
        namespace.create_tenant_namespace(tenant, quantity)
        return redirect(url_for('.login'))
    abort(make_response(jsonify(message='User already exist'), 403))


@auth_routes.route('/logout', methods=['GET', 'POST'])
def logout_user():
    session.clear()
    return make_response('Logged out user', 200)
